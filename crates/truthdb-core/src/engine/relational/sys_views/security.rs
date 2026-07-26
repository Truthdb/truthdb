use super::*;

pub(in crate::engine::relational) fn sys_server_principals(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        int_col("is_disabled"),
    ];
    let mut rows: Vec<Vec<Datum>> = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                // SQL logins: type 'S' / SQL_LOGIN.
                Datum::NVarChar("S".to_string()),
                Datum::NVarChar("SQL_LOGIN".to_string()),
                Datum::Int(i32::from(principal.is_disabled)),
            ])
        })
        .collect();
    // The fixed server roles (today: sysadmin) — type 'R' / SERVER_ROLE.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| p.is_server)
    {
        rows.push(vec![
            Datum::NVarChar(fixed.name.to_string()),
            Datum::Int(fixed.id as i32),
            Datum::NVarChar("R".to_string()),
            Datum::NVarChar("SERVER_ROLE".to_string()),
            Datum::Int(0),
        ]);
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_sql_logins(storage: &Storage) -> Source {
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        int_col("is_disabled"),
    ];
    let rows = storage
        .rel_logins()
        .into_iter()
        .filter_map(|def| {
            let principal = def.principal.as_ref()?;
            Some(vec![
                Datum::NVarChar(def.name.clone()),
                Datum::Int(def.object_id as i32),
                Datum::Int(i32::from(principal.is_disabled)),
            ])
        })
        .collect();
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_database_principals(storage: &Storage) -> Source {
    use crate::relstore::catalog::PrincipalKind;
    let columns = vec![
        nvarchar("name", 128),
        int_col("principal_id"),
        nvarchar("type", 1),
        nvarchar("type_desc", 60),
        nvarchar("default_schema_name", 128),
        int_col("owning_principal_id"),
    ];
    // A user (SQL_USER 'S') defaults to the dbo schema; a role (DATABASE_ROLE
    // 'R') has no default schema.
    let row = |name: String, id: u32, kind: PrincipalKind| {
        let (type_code, type_desc) = match kind {
            PrincipalKind::Role => ("R", "DATABASE_ROLE"),
            _ => ("S", "SQL_USER"),
        };
        let default_schema = if matches!(kind, PrincipalKind::User) {
            Datum::NVarChar("dbo".to_string())
        } else {
            Datum::Null
        };
        vec![
            Datum::NVarChar(name),
            Datum::Int(id as i32),
            Datum::NVarChar(type_code.to_string()),
            Datum::NVarChar(type_desc.to_string()),
            default_schema,
            Datum::Null, // owning_principal_id
        ]
    };
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Fixed database principals (dbo + the fixed database roles + public); the
    // server-scoped sysadmin role belongs to sys.server_principals instead.
    for fixed in crate::storage::FIXED_PRINCIPALS
        .iter()
        .filter(|p| !p.is_server)
    {
        rows.push(row(fixed.name.to_string(), fixed.id, fixed.kind));
    }
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            rows.push(row(def.name.clone(), def.object_id, principal.kind));
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_database_role_members(storage: &Storage) -> Source {
    let columns = vec![int_col("role_principal_id"), int_col("member_principal_id")];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    // Synthesized: the dbo user is a member of db_owner.
    rows.push(vec![
        Datum::Int(crate::storage::DB_OWNER_ID as i32),
        Datum::Int(crate::storage::DBO_ID as i32),
    ]);
    // Stored database membership edges (member -> role, from each member's row).
    for def in storage.rel_database_principals() {
        if let Some(principal) = def.principal.as_ref() {
            for &role_id in &principal.member_of {
                rows.push(vec![
                    Datum::Int(role_id as i32),
                    Datum::Int(def.object_id as i32),
                ]);
            }
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}

pub(in crate::engine::relational) fn sys_database_permissions(
    storage: &Storage,
    db_id: u32,
) -> Source {
    let columns = vec![
        int_col("class"),
        nvarchar("class_desc", 60),
        int_col("major_id"),
        int_col("minor_id"),
        int_col("grantee_principal_id"),
        nvarchar("permission_name", 128),
        nvarchar("state", 1),
        nvarchar("state_desc", 60),
    ];
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    for def in storage.rel_tables() {
        if def.database_id != db_id {
            continue;
        }
        for perm in &def.permissions {
            let (state, state_desc) = if perm.deny {
                ("D", "DENY")
            } else {
                ("G", "GRANT")
            };
            rows.push(vec![
                Datum::Int(1), // class 1 = OBJECT_OR_COLUMN
                Datum::NVarChar("OBJECT_OR_COLUMN".to_string()),
                Datum::Int(def.object_id as i32),
                Datum::Int(0), // minor_id 0 = the whole object (no column-level)
                Datum::Int(perm.grantee as i32),
                Datum::NVarChar(perm.action.name().to_string()),
                Datum::NVarChar(state.to_string()),
                Datum::NVarChar(state_desc.to_string()),
            ]);
        }
    }
    let collations = vec![None; columns.len()];
    let qualifiers = vec![None; columns.len()];
    Source {
        columns,
        qualifiers,
        collations,
        rows: SourceRows::Materialized(rows),
    }
}
