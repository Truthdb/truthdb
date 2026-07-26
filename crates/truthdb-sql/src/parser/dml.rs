use super::*;

impl Parser {
    // ---- INSERT ---------------------------------------------------------

    pub(super) fn parse_insert(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("INSERT")?;
        // Optional INTO.
        if self.peek_keyword().as_deref() == Some("INTO") {
            self.bump();
        }
        let table = self.parse_table_name()?;
        let columns = if self.check(&TokenKind::LParen) {
            // Column list, unless this paren opens VALUES-less tuple (it
            // does not in our grammar), so it is always a column list.
            self.bump();
            let mut names = Vec::new();
            loop {
                names.push(self.parse_name()?);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            self.expect(&TokenKind::RParen)?;
            Some(names)
        } else {
            None
        };
        // The row source is either a SELECT or literal VALUES tuples.
        let source = if self.peek_keyword().as_deref() == Some("SELECT") {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else {
            self.expect_keyword("VALUES")?;
            let mut rows = Vec::new();
            loop {
                self.expect(&TokenKind::LParen)?;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_expr()?);
                    if !self.eat(&TokenKind::Comma) {
                        break;
                    }
                }
                self.expect(&TokenKind::RParen)?;
                rows.push(values);
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
            InsertSource::Values(rows)
        };
        let end = self.prev_span();
        Ok(Statement::Insert(Insert {
            span: start.to(end),
            table,
            columns,
            source,
        }))
    }

    // ---- UPDATE ---------------------------------------------------------

    pub(super) fn parse_update(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("UPDATE")?;
        let table = self.parse_name()?;
        self.expect_keyword("SET")?;
        let mut assignments = Vec::new();
        loop {
            let column = self.parse_name()?;
            self.expect(&TokenKind::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if !self.eat(&TokenKind::Comma) {
                break;
            }
        }
        let where_clause = self.parse_optional_where()?;
        let end = self.prev_span();
        Ok(Statement::Update(Update {
            span: start.to(end),
            table,
            assignments,
            where_clause,
        }))
    }

    // ---- DELETE ---------------------------------------------------------

    pub(super) fn parse_delete(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DELETE")?;
        // Optional FROM.
        if self.peek_keyword().as_deref() == Some("FROM") {
            self.bump();
        }
        let table = self.parse_name()?;
        let where_clause = self.parse_optional_where()?;
        let end = self.prev_span();
        Ok(Statement::Delete(Delete {
            span: start.to(end),
            table,
            where_clause,
        }))
    }

    fn parse_optional_where(&mut self) -> SqlResult<Option<Expr>> {
        if self.peek_keyword().as_deref() == Some("WHERE") {
            self.bump();
            Ok(Some(self.parse_expr()?))
        } else {
            Ok(None)
        }
    }
}
