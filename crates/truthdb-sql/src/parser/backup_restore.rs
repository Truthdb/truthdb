use super::*;

impl Parser {
    /// `CREATE|ALTER LOGIN <name> WITH PASSWORD = '<pw>'` or `ALTER LOGIN <name>
    /// {ENABLE | DISABLE}`.
    pub(super) fn parse_backup(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        self.bump(); // BACKUP
        // BACKUP is a side-effecting operation: it is illegal inside a function
        // or table-valued-function body (a function must be side-effect-free —
        // otherwise `SELECT dbo.f(x) FROM t` would run a full backup per row).
        // It is permitted inside a stored procedure, matching SQL Server.
        if self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'BACKUP'.").at(start),
            );
        }
        let is_log = match self.peek_keyword().as_deref() {
            Some("DATABASE") => false,
            Some("LOG") => true,
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.bump(); // DATABASE | LOG
        let database = self.parse_name()?;
        self.expect_keyword("TO")?;
        self.expect_keyword("DISK")?;
        self.expect(&TokenKind::Eq)?;
        let token = self.peek().clone();
        let TokenKind::String(path) = &token.kind else {
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        };
        let path = path.clone();
        self.bump();

        // Optional `WITH` options. CHECKSUM (default on) / NO_CHECKSUM,
        // COPY_ONLY, and INIT / NOINIT (accepted but inert — a TDBBAK1 file
        // holds one backup, so the destination is always overwritten).
        let mut checksum = true;
        let mut copy_only = false;
        if self.eat_keyword("WITH") {
            loop {
                match self.peek_keyword().as_deref() {
                    Some("CHECKSUM") => {
                        self.bump();
                        checksum = true;
                    }
                    Some("NO_CHECKSUM") => {
                        self.bump();
                        checksum = false;
                    }
                    Some("COPY_ONLY") => {
                        self.bump();
                        copy_only = true;
                    }
                    Some("INIT") | Some("NOINIT") => {
                        self.bump();
                    }
                    _ => {
                        let token = self.peek().clone();
                        return Err(SqlError::syntax(self.token_text(&token), token.span));
                    }
                }
                if !self.eat(&TokenKind::Comma) {
                    break;
                }
            }
        }
        let span = start.to(self.prev_span());
        if is_log {
            Ok(Statement::BackupLog {
                database,
                path,
                checksum,
                copy_only,
                span,
            })
        } else {
            Ok(Statement::BackupDatabase {
                database,
                path,
                checksum,
                copy_only,
                span,
            })
        }
    }

    /// `RESTORE {VERIFYONLY|HEADERONLY|FILELISTONLY|DATABASE <name>|LOG <name>}
    /// FROM DISK = '<path>'`. The three inspect verbs run online; DATABASE/LOG
    /// parse (so the error is semantic, not a syntax error) but restore offline.
    pub(super) fn parse_restore(&mut self) -> SqlResult<Statement> {
        let start = self.peek().span;
        self.bump(); // RESTORE
        // Like BACKUP, RESTORE is side-effecting/privileged and illegal inside a
        // function or table-valued-function body.
        if self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'RESTORE'.").at(start),
            );
        }
        let mode = match self.peek_keyword().as_deref() {
            Some("VERIFYONLY") => {
                self.bump();
                RestoreMode::VerifyOnly
            }
            Some("HEADERONLY") => {
                self.bump();
                RestoreMode::HeaderOnly
            }
            Some("FILELISTONLY") => {
                self.bump();
                RestoreMode::FileListOnly
            }
            Some("DATABASE") => {
                self.bump();
                let _ = self.parse_name()?;
                RestoreMode::Database
            }
            Some("LOG") => {
                self.bump();
                let _ = self.parse_name()?;
                RestoreMode::Log
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.expect_keyword("FROM")?;
        self.expect_keyword("DISK")?;
        self.expect(&TokenKind::Eq)?;
        let token = self.peek().clone();
        let TokenKind::String(path) = &token.kind else {
            return Err(SqlError::syntax(self.token_text(&token), token.span));
        };
        let path = path.clone();
        self.bump();
        let span = start.to(self.prev_span());
        Ok(Statement::Restore { mode, path, span })
    }
}
