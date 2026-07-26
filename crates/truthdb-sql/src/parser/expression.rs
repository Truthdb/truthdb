use super::token::is_reserved;
use super::*;

impl Parser {
    // ---- expressions (precedence climbing) ------------------------------

    /// Expression entry point with a recursion-depth guard (parens, the only
    /// unbounded nesting other than NOT/unary, re-enter here).
    pub(super) fn parse_expr(&mut self) -> SqlResult<Expr> {
        if self.depth == 0 {
            // The node budget is per top-level expression (see
            // MAX_EXPR_NODES): reset it here rather than accumulating across
            // the batch, or flat many-expression statements (a 1000-tuple
            // INSERT) exhaust a budget meant for one deep spine.
            self.nodes = 0;
        }
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(Self::too_deep());
        }
        let expr = self.parse_or()?;
        self.depth -= 1;
        Ok(expr)
    }

    fn parse_or(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_and()?;
        while self.peek_keyword().as_deref() == Some("OR") {
            self.bump();
            let right = self.parse_and()?;
            self.node()?;
            left = binary(BinaryOp::Or, left, right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_not()?;
        while self.peek_keyword().as_deref() == Some("AND") {
            self.bump();
            let right = self.parse_not()?;
            self.node()?;
            left = binary(BinaryOp::And, left, right);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> SqlResult<Expr> {
        if self.peek_keyword().as_deref() == Some("NOT") {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            let start = self.bump().span;
            let expr = self.parse_not()?;
            self.depth -= 1;
            self.node()?;
            return Ok(Expr {
                span: start.to(expr.span),
                kind: ExprKind::Unary {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                },
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> SqlResult<Expr> {
        let left = self.parse_additive()?;
        // IS [NOT] NULL
        if self.peek_keyword().as_deref() == Some("IS") {
            self.bump();
            let negated = if self.peek_keyword().as_deref() == Some("NOT") {
                self.bump();
                true
            } else {
                false
            };
            let end = self.expect_keyword("NULL")?;
            self.node()?;
            return Ok(Expr {
                span: left.span.to(end),
                kind: ExprKind::IsNull {
                    expr: Box::new(left),
                    negated,
                },
            });
        }
        // [NOT] LIKE / IN / BETWEEN (the trailing-NOT predicate form).
        let negated = self.peek_keyword().as_deref() == Some("NOT")
            && matches!(
                self.peek_keyword_at(1).as_deref(),
                Some("LIKE") | Some("IN") | Some("BETWEEN")
            );
        if negated {
            self.bump();
        }
        match self.peek_keyword().as_deref() {
            Some("LIKE") => return self.parse_like(left, negated),
            Some("IN") => return self.parse_in(left, negated),
            Some("BETWEEN") => return self.parse_between(left, negated),
            _ => {}
        }
        let op = match self.peek().kind {
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::Ne => BinaryOp::Ne,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Le => BinaryOp::Le,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::Ge => BinaryOp::Ge,
            _ => return Ok(left),
        };
        self.bump();
        let right = self.parse_additive()?;
        self.node()?;
        Ok(binary(op, left, right))
    }

    fn parse_like(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // LIKE
        let pattern = self.parse_additive()?;
        let mut end = pattern.span;
        let escape = if self.peek_keyword().as_deref() == Some("ESCAPE") {
            self.bump();
            let token = self.bump();
            end = token.span;
            match &token.kind {
                TokenKind::String(s) if s.chars().count() == 1 => s.chars().next(),
                _ => return Err(SqlError::syntax(self.token_text(&token), token.span)),
            }
        } else {
            None
        };
        self.node()?;
        Ok(Expr {
            span: left.span.to(end),
            kind: ExprKind::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                escape,
                negated,
            },
        })
    }

    fn parse_in(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // IN
        self.expect(&TokenKind::LParen)?;
        // `expr IN (SELECT ...)` is a subquery; otherwise a value list.
        if self.peek_keyword().as_deref() == Some("SELECT") {
            let subquery = self.parse_select()?;
            let end = self.expect(&TokenKind::RParen)?;
            self.node()?;
            return Ok(Expr {
                span: left.span.to(end),
                kind: ExprKind::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
                    negated,
                },
            });
        }
        let mut list = Vec::new();
        loop {
            list.push(self.parse_expr()?);
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: left.span.to(end),
            kind: ExprKind::InList {
                expr: Box::new(left),
                list,
                negated,
            },
        })
    }

    fn parse_between(&mut self, left: Expr, negated: bool) -> SqlResult<Expr> {
        self.bump(); // BETWEEN
        // `low`/`high` parse at additive precedence so BETWEEN's `AND` is not
        // swallowed as a boolean connective.
        let low = self.parse_additive()?;
        self.expect_keyword("AND")?;
        let high = self.parse_additive()?;
        self.node()?;
        Ok(Expr {
            span: left.span.to(high.span),
            kind: ExprKind::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated,
            },
        })
    }

    fn parse_function(&mut self, name: Name) -> SqlResult<Expr> {
        self.expect(&TokenKind::LParen)?;
        if let Some(func) = agg_func(&name.value) {
            return self.parse_aggregate(name, func);
        }
        let mut args = Vec::new();
        if !self.check(&TokenKind::RParen) {
            loop {
                args.push(self.parse_expr()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: name.span.to(end),
            kind: ExprKind::Function {
                name: name.value,
                args,
            },
        })
    }

    /// Parses an aggregate call body (the opening `(` is already consumed):
    /// `COUNT(*)`, `COUNT([DISTINCT|ALL] expr)`, `SUM/AVG/MIN/MAX(...)`.
    fn parse_aggregate(&mut self, name: Name, func: AggFunc) -> SqlResult<Expr> {
        // COUNT(*) — the only aggregate that takes a star.
        if func == AggFunc::Count && self.check(&TokenKind::Star) {
            self.bump();
            let end = self.expect(&TokenKind::RParen)?;
            self.node()?;
            return Ok(Expr {
                span: name.span.to(end),
                kind: ExprKind::Aggregate {
                    func,
                    distinct: false,
                    arg: None,
                },
            });
        }
        let distinct = match self.peek_keyword().as_deref() {
            Some("DISTINCT") => {
                self.bump();
                true
            }
            Some("ALL") => {
                self.bump();
                false
            }
            _ => false,
        };
        let arg = self.parse_expr()?;
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: name.span.to(end),
            kind: ExprKind::Aggregate {
                func,
                distinct,
                arg: Some(Box::new(arg)),
            },
        })
    }

    fn parse_case(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CASE
        // A simple CASE has an operand before the first WHEN.
        let operand = if self.peek_keyword().as_deref() == Some("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut branches = Vec::new();
        while self.peek_keyword().as_deref() == Some("WHEN") {
            self.bump();
            let cond = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let result = self.parse_expr()?;
            self.node()?;
            branches.push((cond, result));
        }
        if branches.is_empty() {
            let token = self.peek().clone();
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        }
        let else_result = if self.peek_keyword().as_deref() == Some("ELSE") {
            self.bump();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let end = self.expect_keyword("END")?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Case {
                operand,
                branches,
                else_result,
            },
        })
    }

    fn parse_cast(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CAST
        self.expect(&TokenKind::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword("AS")?;
        let (target, _) = self.parse_data_type()?;
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target,
            },
        })
    }

    fn parse_convert(&mut self) -> SqlResult<Expr> {
        let start = self.bump().span; // CONVERT
        self.expect(&TokenKind::LParen)?;
        let (target, _) = self.parse_data_type()?;
        self.expect(&TokenKind::Comma)?;
        let expr = self.parse_expr()?;
        // An optional style argument is accepted and ignored for now.
        if self.eat(&TokenKind::Comma) {
            let _ = self.parse_expr()?;
        }
        let end = self.expect(&TokenKind::RParen)?;
        self.node()?;
        Ok(Expr {
            span: start.to(end),
            kind: ExprKind::Cast {
                expr: Box::new(expr),
                target,
            },
        })
    }

    fn parse_additive(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek().kind {
                TokenKind::Plus => BinaryOp::Add,
                TokenKind::Minus => BinaryOp::Sub,
                _ => break,
            };
            self.bump();
            let right = self.parse_multiplicative()?;
            self.node()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> SqlResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek().kind {
                TokenKind::Star => BinaryOp::Mul,
                TokenKind::Slash => BinaryOp::Div,
                TokenKind::Percent => BinaryOp::Mod,
                _ => break,
            };
            self.bump();
            let right = self.parse_unary()?;
            self.node()?;
            left = binary(op, left, right);
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> SqlResult<Expr> {
        if self.check(&TokenKind::Minus) {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            let start = self.bump().span;
            let expr = self.parse_unary()?;
            self.depth -= 1;
            self.node()?;
            return Ok(Expr {
                span: start.to(expr.span),
                kind: ExprKind::Unary {
                    op: UnaryOp::Neg,
                    expr: Box::new(expr),
                },
            });
        }
        if self.check(&TokenKind::Plus) {
            self.depth += 1;
            if self.depth > MAX_EXPR_DEPTH {
                return Err(Self::too_deep());
            }
            self.bump();
            let expr = self.parse_unary()?;
            self.depth -= 1;
            return Ok(expr);
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> SqlResult<Expr> {
        self.node()?;
        let token = self.peek().clone();
        match &token.kind {
            TokenKind::Int(v) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Int(*v),
                    span: token.span,
                })
            }
            TokenKind::Number(text) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Number(text.clone()),
                    span: token.span,
                })
            }
            TokenKind::String(s) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::Str(s.clone()),
                    span: token.span,
                })
            }
            TokenKind::GlobalVar(name) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::GlobalVar(name.clone()),
                    span: token.span,
                })
            }
            TokenKind::LocalVar(name) => {
                self.bump();
                Ok(Expr {
                    kind: ExprKind::LocalVar(name.clone()),
                    span: token.span,
                })
            }
            TokenKind::LParen => {
                // `(SELECT ...)` is a scalar subquery; otherwise a grouping paren.
                if self.peek_keyword_at(1).as_deref() == Some("SELECT") {
                    let start = self.bump().span; // (
                    let subquery = self.parse_select()?;
                    let end = self.expect(&TokenKind::RParen)?;
                    Ok(Expr {
                        kind: ExprKind::Subquery(Box::new(subquery)),
                        span: start.to(end),
                    })
                } else {
                    self.bump();
                    let inner = self.parse_expr()?;
                    self.expect(&TokenKind::RParen)?;
                    Ok(inner)
                }
            }
            TokenKind::Word { quoted, .. } => {
                let keyword = token.keyword();
                match keyword.as_deref() {
                    Some("NULL") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Null,
                            span: token.span,
                        })
                    }
                    Some("TRUE") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Bool(true),
                            span: token.span,
                        })
                    }
                    Some("FALSE") if !quoted => {
                        self.bump();
                        Ok(Expr {
                            kind: ExprKind::Bool(false),
                            span: token.span,
                        })
                    }
                    Some("CASE") if !quoted => self.parse_case(),
                    Some("CAST") if !quoted => self.parse_cast(),
                    Some("CONVERT") if !quoted => self.parse_convert(),
                    Some("EXISTS") if !quoted => {
                        // `EXISTS (SELECT ...)`; `NOT EXISTS` is parse_not over this.
                        let start = self.bump().span;
                        self.expect(&TokenKind::LParen)?;
                        let subquery = self.parse_select()?;
                        let end = self.expect(&TokenKind::RParen)?;
                        Ok(Expr {
                            kind: ExprKind::Exists(Box::new(subquery)),
                            span: start.to(end),
                        })
                    }
                    Some(kw) if !quoted && is_reserved(kw) => {
                        Err(SqlError::syntax(self.token_text(&token), token.span))
                    }
                    _ => {
                        let name = self.parse_name()?;
                        // A name followed by `(` is a function call — including a
                        // schema-qualified one (`dbo.f(@x)`), the canonical way to
                        // call a user-defined function. A column reference is
                        // never followed by `(`, so this never misreads one.
                        if self.check(&TokenKind::LParen) {
                            self.parse_function(name)
                        } else {
                            Ok(Expr {
                                span: name.span,
                                kind: ExprKind::Column(name),
                            })
                        }
                    }
                }
            }
            _ => Err(SqlError::syntax(self.token_text(&token), token.span)),
        }
    }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr {
        span: left.span.to(right.span),
        kind: ExprKind::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
    }
}

/// The aggregate function for a name, if it is one (case-insensitive).
fn agg_func(name: &str) -> Option<AggFunc> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    }
}
