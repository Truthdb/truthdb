use super::*;

mod schema;

impl Parser {
    // ---- CREATE TABLE ---------------------------------------------------

    /// Dispatches `CREATE TABLE` vs `CREATE [UNIQUE] INDEX`.
    pub(super) fn parse_create(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("CREATE")?;
        let unique = self.peek_keyword().as_deref() == Some("UNIQUE");
        if unique {
            self.bump();
        }
        match self.peek_keyword().as_deref() {
            Some("INDEX") => self.parse_create_index(start, unique),
            Some("TABLE") if !unique => self.parse_create_table(start),
            Some("VIEW") if !unique => self.parse_create_view(start),
            Some("PROCEDURE") | Some("PROC") if !unique => {
                self.parse_create_procedure(start, false)
            }
            Some("FUNCTION") if !unique => self.parse_create_function(start, false),
            Some("TRIGGER") if !unique => self.parse_create_trigger(start, false),
            Some("LOGIN") if !unique => self.parse_create_login(start, false),
            Some("USER") if !unique => self.parse_create_user(start),
            Some("ROLE") if !unique => self.parse_create_role(start),
            Some("DATABASE") if !unique => self.parse_create_database(start),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }

    // ---- ALTER TABLE ----------------------------------------------------

    pub(super) fn parse_alter(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("ALTER")?;
        if self.peek_keyword().as_deref() == Some("DATABASE") {
            return self.parse_alter_database(start);
        }
        if matches!(
            self.peek_keyword().as_deref(),
            Some("PROCEDURE") | Some("PROC")
        ) {
            return self.parse_create_procedure(start, true);
        }
        if self.peek_keyword().as_deref() == Some("FUNCTION") {
            return self.parse_create_function(start, true);
        }
        if self.peek_keyword().as_deref() == Some("TRIGGER") {
            return self.parse_create_trigger(start, true);
        }
        if self.peek_keyword().as_deref() == Some("LOGIN") {
            return self.parse_create_login(start, true);
        }
        if self.peek_keyword().as_deref() == Some("ROLE") {
            return self.parse_alter_role(start);
        }
        self.expect_keyword("TABLE")?;
        let table = self.parse_name()?;
        let (action, end) = match self.peek_keyword().as_deref() {
            Some("ADD") => {
                self.bump();
                // `ADD [CONSTRAINT name] (CHECK | FOREIGN KEY ...)`, or
                // `ADD <column> <type> ...` — T-SQL has no COLUMN keyword
                // here, so anything but a constraint introducer is a column.
                match self.peek_keyword().as_deref() {
                    Some("CONSTRAINT") | Some("FOREIGN") | Some("CHECK") => {
                        let name = self.parse_optional_constraint_name()?;
                        if self.peek_keyword().as_deref() == Some("FOREIGN") {
                            let fk = self.parse_foreign_key(name)?;
                            let end = fk.span;
                            (AlterAction::AddForeignKey(fk), end)
                        } else {
                            let check = self.parse_check_constraint(name)?;
                            let end = check.span;
                            (AlterAction::AddCheck(check), end)
                        }
                    }
                    _ => {
                        let column = self.parse_column_def()?;
                        let end = column.span;
                        (AlterAction::AddColumn(column), end)
                    }
                }
            }
            Some("DROP") => {
                self.bump();
                self.expect_keyword("CONSTRAINT")?;
                let name = self.parse_name()?;
                let end = name.span;
                (AlterAction::DropConstraint(name), end)
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        Ok(Statement::AlterTable(AlterTable {
            table,
            action,
            span: start.to(end),
        }))
    }

    // ---- DROP TABLE -----------------------------------------------------

    /// Dispatches `DROP TABLE` vs `DROP INDEX`.
    pub(super) fn parse_drop(&mut self) -> SqlResult<Statement> {
        let start = self.expect_keyword("DROP")?;
        match self.peek_keyword().as_deref() {
            Some("INDEX") => self.parse_drop_index(start),
            Some("TABLE") => self.parse_drop_table(start),
            Some("VIEW") => self.parse_drop_view(start),
            Some("PROCEDURE") | Some("PROC") => self.parse_drop_procedure(start),
            Some("FUNCTION") => self.parse_drop_function(start),
            Some("TRIGGER") => self.parse_drop_trigger(start),
            Some("LOGIN") => self.parse_drop_login(start),
            Some("USER") => self.parse_drop_user(start),
            Some("ROLE") => self.parse_drop_role(start),
            Some("DATABASE") => self.parse_drop_database(start),
            _ => {
                let token = self.peek().clone();
                Err(SqlError::syntax(self.token_text(&token), token.span))
            }
        }
    }
}
