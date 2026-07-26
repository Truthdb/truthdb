use super::super::prelude::*;

pub(in crate::engine::relational) fn exec_create_login(
    storage: &Storage,
    create: &CreateLogin,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if create.alter {
        let Some(existing) = storage.rel_login(bare) else {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot alter the login '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(create.name.span));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("rel_login returns a login");
        if let Some(password) = &create.password {
            principal.password_blob = crate::auth::hash_password(password);
        }
        if let Some(disable) = create.disable {
            principal.is_disabled = disable;
        }
        storage
            .rel_alter_login(bare, principal)
            .map_err(|e| map_storage_err(e, &create.name.value))?;
        return Ok(StatementResult::Done);
    }
    if storage.rel_login(bare).is_some() {
        return Err(SqlError::new(
            15025,
            16,
            1,
            format!("The server principal '{bare}' already exists."),
        )
        .at(create.name.span));
    }
    let password = create
        .password
        .as_ref()
        .expect("CREATE LOGIN carries a password (parser-enforced)");
    let principal = PrincipalDef::login(
        crate::auth::hash_password(password),
        create.disable.unwrap_or(false),
    );
    storage
        .rel_create_login(bare, principal)
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

pub(in crate::engine::relational) fn exec_drop_login(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let dropped = storage
        .rel_drop_login(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    if !dropped && !if_exists {
        return Err(SqlError::new(
            15151,
            16,
            1,
            format!(
                "Cannot drop the login '{bare}', because it does not exist or you do not have permission."
            ),
        )
        .at(name.span));
    }
    Ok(StatementResult::Done)
}

/// `CREATE USER <name> [FOR LOGIN <login>]`. A database principal in its own
/// namespace (out of the object namespace), optionally mapped to a login.
pub(in crate::engine::relational) fn exec_create_user(
    storage: &Storage,
    create: &CreateUser,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&create.name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(create.name.span));
    }
    let login_sid = match &create.for_login {
        Some(login) => {
            let login_bare = strip_schema(&login.value);
            let Some(def) = storage.rel_login(login_bare) else {
                return Err(SqlError::new(
                    15007,
                    16,
                    1,
                    format!("'{login_bare}' is not a valid login or you do not have permission."),
                )
                .at(login.span));
            };
            Some(def.object_id)
        }
        None => None,
    };
    storage
        .rel_create_database_principal(bare, PrincipalDef::user(login_sid))
        .map_err(|e| map_storage_err(e, &create.name.value))?;
    Ok(StatementResult::Done)
}

/// `CREATE ROLE <name>`.
pub(in crate::engine::relational) fn exec_create_role(
    storage: &Storage,
    name: &Name,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    if storage.rel_database_principal(bare).is_some()
        || crate::storage::fixed_principal_by_name(bare).is_some()
    {
        return Err(SqlError::new(
            15023,
            16,
            1,
            format!("User, group, or role '{bare}' already exists in the current database."),
        )
        .at(name.span));
    }
    storage
        .rel_create_database_principal(bare, PrincipalDef::role())
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `DROP USER`/`DROP ROLE`. `expect_role` selects which kind is being dropped;
/// a mismatch (DROP USER on a role, or vice versa) reports not-found for the
/// requested kind, as SQL Server does.
pub(in crate::engine::relational) fn exec_drop_database_principal(
    storage: &Storage,
    name: &Name,
    if_exists: bool,
    expect_role: bool,
) -> Result<StatementResult, SqlError> {
    let bare = strip_schema(&name.value);
    let kind = if expect_role { "role" } else { "user" };
    match storage.rel_database_principal(bare) {
        Some(def) if def.is_role() == expect_role => {}
        _ if if_exists => return Ok(StatementResult::Done),
        _ => {
            return Err(SqlError::new(
                15151,
                16,
                1,
                format!(
                    "Cannot drop the {kind} '{bare}', because it does not exist or you do not have permission."
                ),
            )
            .at(name.span));
        }
    }
    storage
        .rel_drop_database_principal(bare)
        .map_err(|e| map_storage_err(e, &name.value))?;
    Ok(StatementResult::Done)
}

/// `ALTER ROLE <role> ADD|DROP MEMBER <member>`.
pub(in crate::engine::relational) fn exec_alter_role_member(
    storage: &Storage,
    role: &Name,
    action: RoleMemberAction,
    member: &Name,
) -> Result<StatementResult, SqlError> {
    let role_bare = strip_schema(&role.value);
    let member_bare = strip_schema(&member.value);
    match action {
        RoleMemberAction::Add => storage.rel_add_role_member(role_bare, member_bare),
        RoleMemberAction::Drop => storage.rel_drop_role_member(role_bare, member_bare),
    }
    .map_err(|e| map_storage_err(e, &role.value))?;
    Ok(StatementResult::Done)
}

/// Maps a parsed permission action to its catalog form.
pub(in crate::engine::relational) fn map_perm_action(action: PermissionAction) -> PermAction {
    match action {
        PermissionAction::Select => PermAction::Select,
        PermissionAction::Insert => PermAction::Insert,
        PermissionAction::Update => PermAction::Update,
        PermissionAction::Delete => PermAction::Delete,
        PermissionAction::Execute => PermAction::Execute,
        PermissionAction::References => PermAction::References,
        PermissionAction::Alter => PermAction::Alter,
    }
}

/// `GRANT|DENY|REVOKE <actions> ON <object> TO|FROM <grantees>`. The authority to
/// manage permissions is enforced by the DDL privilege gate in the dispatcher
/// (a bypassing principal — sysadmin / dbo / db_owner / internal). Here we just
/// resolve the securable and apply each (grantee, action).
pub(in crate::engine::relational) fn exec_permission(
    storage: &Storage,
    db_id: u32,
    stmt: &PermissionStatement,
    _sec: &SecurityContext,
) -> Result<StatementResult, SqlError> {
    // The securable must be a schema object (table, view, procedure, function).
    let Some(def) = resolve_table(storage, db_id, &stmt.object.value) else {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    };
    if def.is_trigger() {
        return Err(SqlError::invalid_object(&stmt.object.value).at(stmt.object.span));
    }
    let object = def.name.clone(); // the canonical name = the rel.tables key
    for grantee in &stmt.grantees {
        let grantee_bare = strip_schema(&grantee.value);
        for action in &stmt.actions {
            let catalog_action = map_perm_action(*action);
            match stmt.kind {
                PermissionKind::Grant => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    false,
                ),
                PermissionKind::Deny => storage.rel_grant_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                    true,
                ),
                PermissionKind::Revoke => storage.rel_revoke_object(
                    def.database_id,
                    &object,
                    grantee_bare,
                    catalog_action,
                ),
            }
            .map_err(|e| map_storage_err(e, &grantee.value).at(grantee.span))?;
        }
    }
    Ok(StatementResult::Done)
}

/// Schema and security DDL a non-privileged principal may not run. (GRANT/DENY/
/// REVOKE — `Permission` — is included: only a privileged principal manages
/// permissions.) Fine-grained database-scoped CREATE grants and the db_ddladmin
/// role are deferred: today any DDL requires bypass privilege.
pub(in crate::engine::relational) fn is_privileged_ddl(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateTable(_)
            | Statement::DropTable(_)
            | Statement::CreateView(_)
            | Statement::DropView(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::AlterTable(_)
            | Statement::AlterDatabase(_)
            | Statement::CreateDatabase { .. }
            | Statement::DropDatabase { .. }
            | Statement::CreateProcedure(_)
            | Statement::DropProcedure { .. }
            | Statement::CreateFunction(_)
            | Statement::DropFunction { .. }
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger { .. }
            | Statement::CreateLogin(_)
            | Statement::DropLogin { .. }
            | Statement::CreateUser(_)
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::AlterRole { .. }
            | Statement::Permission(_)
            | Statement::SetTriggerState { .. }
            | Statement::BackupDatabase { .. }
            | Statement::BackupLog { .. }
            | Statement::Restore { .. }
    )
}
