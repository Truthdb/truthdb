use super::*;

impl Parser {
    pub(super) fn parse_create_login(&mut self, start: Span, alter: bool) -> SqlResult<Statement> {
        self.bump(); // LOGIN
        if self.in_procedure || self.in_function || self.in_table_function {
            return Err(
                SqlError::new(156, 15, 1, "Incorrect syntax near the keyword 'LOGIN'.").at(start),
            );
        }
        if self.statement_index > 0 || self.sub_depth > 0 {
            return Err(SqlError::new(
                111,
                15,
                1,
                "'CREATE/ALTER LOGIN' must be the first statement in a query batch.",
            )
            .at(start));
        }
        let name = self.parse_name()?;
        let mut password = None;
        let mut disable = None;
        if alter
            && matches!(
                self.peek_keyword().as_deref(),
                Some("ENABLE") | Some("DISABLE")
            )
        {
            disable = Some(self.peek_keyword().as_deref() == Some("DISABLE"));
            self.bump();
        } else {
            self.expect_keyword("WITH")?;
            self.expect_keyword("PASSWORD")?;
            self.expect(&TokenKind::Eq)?;
            let token = self.peek().clone();
            let TokenKind::String(pw) = &token.kind else {
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            };
            password = Some(pw.clone());
            self.bump();
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateLogin(CreateLogin {
            name,
            password,
            disable,
            alter,
            span,
        }))
    }

    /// `CREATE DATABASE <name>` — a bare (single-part) name; SQL Server's
    /// storage clauses (`ON`, `LOG ON`, ...) do not apply to a shared-file
    /// instance and are not accepted.
    pub(super) fn parse_create_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // DATABASE
        let name = self.parse_single_part_name()?;
        Ok(Statement::CreateDatabase {
            span: start.to(name.span),
            name,
        })
    }

    /// `DROP DATABASE [IF EXISTS] <name>`.
    pub(super) fn parse_drop_database(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // DATABASE
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_single_part_name()?;
        Ok(Statement::DropDatabase {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// A name that must be a single identifier — a database name is never
    /// dotted (error 170-class syntax rejection on a qualifier).
    pub(super) fn parse_single_part_name(&mut self) -> SqlResult<Name> {
        let name = self.parse_name()?;
        if name.value.contains('.') {
            return Err(SqlError::syntax(&name.value, name.span));
        }
        Ok(name)
    }

    pub(super) fn parse_drop_login(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // LOGIN
        let if_exists = if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropLogin {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `CREATE USER <name> [FOR LOGIN <login> | WITHOUT LOGIN]`.
    pub(super) fn parse_create_user(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // USER
        let name = self.parse_name()?;
        let mut for_login = None;
        match self.peek_keyword().as_deref() {
            Some("FOR") => {
                self.bump();
                self.expect_keyword("LOGIN")?;
                for_login = Some(self.parse_name()?);
            }
            // `WITHOUT LOGIN` — a user with no mapped login (accepted, no map).
            Some("WITHOUT") => {
                self.bump();
                self.expect_keyword("LOGIN")?;
            }
            _ => {}
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateUser(CreateUser {
            name,
            for_login,
            span,
        }))
    }

    /// `CREATE ROLE <name> [AUTHORIZATION <owner>]` (AUTHORIZATION accepted, ignored).
    pub(super) fn parse_create_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let name = self.parse_name()?;
        if self.peek_keyword().as_deref() == Some("AUTHORIZATION") {
            self.bump();
            let _ = self.parse_name()?;
        }
        let span = start.to(self.prev_span());
        Ok(Statement::CreateRole { name, span })
    }

    pub(super) fn parse_drop_user(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // USER
        let if_exists = self.parse_optional_if_exists()?;
        let name = self.parse_name()?;
        Ok(Statement::DropUser {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    pub(super) fn parse_drop_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let if_exists = self.parse_optional_if_exists()?;
        let name = self.parse_name()?;
        Ok(Statement::DropRole {
            span: start.to(name.span),
            name,
            if_exists,
        })
    }

    /// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
    pub(super) fn parse_alter_role(&mut self, start: Span) -> SqlResult<Statement> {
        self.bump(); // ROLE
        let name = self.parse_name()?;
        let action = match self.peek_keyword().as_deref() {
            Some("ADD") => {
                self.bump();
                RoleMemberAction::Add
            }
            Some("DROP") => {
                self.bump();
                RoleMemberAction::Drop
            }
            _ => {
                let token = self.peek().clone();
                return Err(SqlError::syntax(self.token_text(&token), token.span));
            }
        };
        self.expect_keyword("MEMBER")?;
        let member = self.parse_name()?;
        let span = start.to(member.span);
        Ok(Statement::AlterRole {
            name,
            action,
            member,
            span,
        })
    }

    /// `GRANT|DENY|REVOKE <action>[, …] ON <object> TO|FROM <grantee>[, …]`.
    /// (Column-level, `WITH GRANT OPTION`, and database-scoped grants are not
    /// parsed here.)
    pub(in crate::parser) fn parse_permission(
        &mut self,
        kind: PermissionKind,
    ) -> SqlResult<Statement> {
        let start = self.expect_keyword(match kind {
            PermissionKind::Grant => "GRANT",
            PermissionKind::Deny => "DENY",
            PermissionKind::Revoke => "REVOKE",
        })?;
        let mut actions = vec![self.parse_permission_action()?];
        while self.peek().kind == TokenKind::Comma {
            self.bump();
            actions.push(self.parse_permission_action()?);
        }
        self.expect_keyword("ON")?;
        let object = self.parse_name()?;
        // GRANT/DENY use TO; REVOKE uses FROM.
        match kind {
            PermissionKind::Revoke => self.expect_keyword("FROM")?,
            _ => self.expect_keyword("TO")?,
        };
        let mut grantees = vec![self.parse_name()?];
        while self.peek().kind == TokenKind::Comma {
            self.bump();
            grantees.push(self.parse_name()?);
        }
        let span = start.to(self.prev_span());
        Ok(Statement::Permission(PermissionStatement {
            kind,
            actions,
            object,
            grantees,
            span,
        }))
    }

    pub(super) fn parse_permission_action(&mut self) -> SqlResult<PermissionAction> {
        let token = self.peek().clone();
        let action = match self.peek_keyword().as_deref() {
            Some("SELECT") => PermissionAction::Select,
            Some("INSERT") => PermissionAction::Insert,
            Some("UPDATE") => PermissionAction::Update,
            Some("DELETE") => PermissionAction::Delete,
            Some("EXECUTE") | Some("EXEC") => PermissionAction::Execute,
            Some("REFERENCES") => PermissionAction::References,
            Some("ALTER") => PermissionAction::Alter,
            _ => return Err(SqlError::syntax(self.token_text(&token), token.span)),
        };
        self.bump();
        Ok(action)
    }

    /// Parses an optional leading `IF EXISTS`.
    pub(super) fn parse_optional_if_exists(&mut self) -> SqlResult<bool> {
        if self.peek_keyword().as_deref() == Some("IF") {
            self.bump();
            self.expect_keyword("EXISTS")?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
