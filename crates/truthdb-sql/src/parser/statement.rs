use super::*;

impl Parser {
    /// Parses a whole batch of `;`-separated statements.
    pub fn parse_statements(mut self) -> SqlResult<Vec<Statement>> {
        let mut statements = Vec::new();
        loop {
            while self.eat(&TokenKind::Semicolon) {}
            if self.at_eof() {
                break;
            }
            // The expression-node budget also resets per statement: CTE and
            // derived-table bodies parse under depth >= 1, so their
            // expressions never reach parse_expr's depth-0 reset and would
            // otherwise inherit the previous statement's count.
            self.nodes = 0;
            self.statement_index = statements.len();
            statements.push(self.parse_statement()?);
            // A label (`<name>:`) prefixes the statement that follows it, so no
            // separator is required after it; every other statement needs `;` or
            // end-of-batch before the next one.
            let after_label = matches!(statements.last(), Some(Statement::Label { .. }));
            if !after_label && !self.at_eof() && !self.check(&TokenKind::Semicolon) {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        }
        Ok(statements)
    }

    pub(super) fn parse_statement(&mut self) -> SqlResult<Statement> {
        // A label `<identifier>:` at statement start (a GOTO target). At the
        // start of a statement, an identifier immediately followed by `:` can
        // only be a label — no statement begins that way.
        if matches!(self.peek().kind, TokenKind::Word { .. })
            && matches!(
                self.tokens.get(self.pos + 1).map(|t| &t.kind),
                Some(TokenKind::Colon)
            )
        {
            return self.parse_label();
        }
        match self.peek_keyword().as_deref() {
            Some("CREATE") => self.parse_create(),
            Some("ALTER") => self.parse_alter(),
            Some("DROP") => self.parse_drop(),
            Some("INSERT") => self.parse_insert(),
            Some("UPDATE") => self.parse_update(),
            Some("DELETE") => self.parse_delete(),
            Some("SELECT") | Some("WITH") => Ok(Statement::Select(self.parse_select()?)),
            Some("BEGIN") => self.parse_begin(),
            Some("COMMIT") => self.parse_commit(),
            Some("ROLLBACK") => self.parse_rollback(),
            Some("SAVE") => self.parse_save(),
            Some("SET") => self.parse_set(),
            Some("DECLARE") => self.parse_declare(),
            Some("EXEC") | Some("EXECUTE") => self.parse_exec(),
            Some("USE") => self.parse_use(),
            Some("THROW") => self.parse_throw(),
            Some("RAISERROR") => self.parse_raiserror(),
            Some("IF") => self.parse_if(),
            Some("WHILE") => self.parse_while(),
            Some("BREAK") => self.parse_break(),
            Some("CONTINUE") => self.parse_continue(),
            Some("RETURN") => self.parse_return(),
            Some("GRANT") => self.parse_permission(PermissionKind::Grant),
            Some("DENY") => self.parse_permission(PermissionKind::Deny),
            Some("REVOKE") => self.parse_permission(PermissionKind::Revoke),
            Some("BACKUP") => self.parse_backup(),
            Some("RESTORE") => self.parse_restore(),
            Some("ENABLE") | Some("DISABLE") => self.parse_trigger_state(),
            Some("GOTO") => self.parse_goto(),
            Some("OPEN") => {
                self.parse_cursor_verb("OPEN", |name, span| Statement::OpenCursor { name, span })
            }
            Some("FETCH") => self.parse_fetch(),
            Some("CLOSE") => {
                self.parse_cursor_verb("CLOSE", |name, span| Statement::CloseCursor { name, span })
            }
            Some("DEALLOCATE") => self.parse_cursor_verb("DEALLOCATE", |name, span| {
                Statement::DeallocateCursor { name, span }
            }),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }
}
