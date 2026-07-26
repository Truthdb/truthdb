use super::*;

#[test]
fn create_login_persists_survives_restart_and_stays_out_of_the_object_namespace() {
    let path = unique_temp_path("login-ddl");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN alice WITH PASSWORD = 'S3cret!'")
        .expect("create login");
    // sys.server_principals shows it as a SQL login.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT name, type, is_disabled FROM sys.server_principals WHERE name = 'alice'",
    );
    assert_eq!(
        rows,
        vec![vec![
            Some("alice".to_string()),
            Some("S".to_string()),
            Some("0".to_string())
        ]],
        "the login appears in sys.server_principals"
    );
    // It is NOT a schema object: not in sys.tables, not queryable as a table.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.tables WHERE name = 'alice'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "a login is not a table"
    );
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "SELECT * FROM alice");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(208),
        "a login name is not a queryable object: {:?}",
        out.error
    );
    // Survives a restart (persisted in the catalog b-tree).
    drop(engine);
    let storage = Storage::open(path.clone()).expect("reopen");
    let engine = Engine::new(storage).expect("replay");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT name FROM sys.server_principals WHERE name = 'alice'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("alice".to_string())]],
        "the login survives restart"
    );

    // ALTER LOGIN ... DISABLE.
    engine
        .execute("ALTER LOGIN alice DISABLE")
        .expect("disable");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT is_disabled FROM sys.sql_logins WHERE name = 'alice'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("1".to_string())]],
        "ALTER DISABLE sets is_disabled"
    );

    // DROP LOGIN.
    engine.execute("DROP LOGIN alice").expect("drop login");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = 'alice'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "DROP LOGIN removes it"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn migrate_logins_is_idempotent_ensures_sa_last_and_survives_config_case_dups() {
    use std::collections::BTreeMap;
    let path = unique_temp_path("login-migrate");
    let engine = new_engine(&path);

    // Case-variant duplicate keys and a lowercase app user; NO sa configured.
    // The dup must NOT error the migration — the second is collapsed onto the
    // first-seen login (names are case-insensitive) — and sa is created LAST,
    // disabled, because no password was configured.
    let mut users = BTreeMap::new();
    users.insert("Admin".to_string(), "p1".to_string());
    users.insert("admin".to_string(), "p2".to_string());
    users.insert("app".to_string(), "app-pw".to_string());
    let created = engine.migrate_logins(&users).expect("first migration");
    assert!(
        created.iter().any(|c| c.starts_with("sa (disabled")),
        "sa is ensured disabled when unconfigured: {created:?}"
    );

    // Exactly one of the case-variant admins exists (the first-sorted, Admin),
    // plus app and sa — the dup did not create a second principal. Filter to
    // SQL logins (type 'S') so the synthesized sysadmin server role does not
    // appear.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT name, is_disabled FROM sys.server_principals WHERE type = 'S' ORDER BY name",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("Admin".to_string()), Some("0".to_string())],
            vec![Some("app".to_string()), Some("0".to_string())],
            vec![Some("sa".to_string()), Some("1".to_string())],
        ],
        "case-dup collapsed to one login; sa present and disabled: {rows:?}"
    );

    // Idempotent: a second run is a no-op (sa exists → the whole thing skips),
    // and it does NOT resurrect a login the admin dropped.
    engine.execute("DROP LOGIN app").expect("drop app");
    let again = engine.migrate_logins(&users).expect("second migration");
    assert!(again.is_empty(), "re-run is a no-op: {again:?}");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.server_principals WHERE name = 'app'",
    );
    assert_eq!(
        rows,
        vec![vec![Some("0".to_string())]],
        "a dropped login is not resurrected by re-migration"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn migrate_logins_uses_the_configured_sa_password_and_enables_it() {
    use std::collections::BTreeMap;
    let path = unique_temp_path("login-migrate-sa");
    let engine = new_engine(&path);
    let mut users = BTreeMap::new();
    users.insert("sa".to_string(), "secret".to_string());
    engine.migrate_logins(&users).expect("migration");
    let rec = engine.lookup_login("sa").expect("sa exists");
    assert!(!rec.is_disabled, "configured sa is enabled");
    assert_eq!(
        crate::auth::verify_password(&rec.password_blob, "secret"),
        crate::auth::VerifyOutcome::Ok,
        "sa authenticates with its configured password"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn database_users_and_roles_persist_and_stay_out_of_the_object_namespace() {
    let path = unique_temp_path("principals");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .expect("login");
    engine
        .execute("CREATE USER app FOR LOGIN sa")
        .expect("user");
    engine.execute("CREATE ROLE reporting").expect("role");

    // sys.database_principals shows the fixed dbo user and db_owner role plus
    // the created user ('S'=SQL_USER) and role ('R'=DATABASE_ROLE).
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT name, type FROM sys.database_principals \
             WHERE name IN ('dbo','db_owner','app','reporting') ORDER BY name",
    );
    assert_eq!(
        rows,
        vec![
            vec![Some("app".into()), Some("S".into())],
            vec![Some("db_owner".into()), Some("R".into())],
            vec![Some("dbo".into()), Some("S".into())],
            vec![Some("reporting".into()), Some("R".into())],
        ]
    );

    // They are not schema objects.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.tables WHERE name IN ('app','reporting')",
    );
    assert_eq!(rows, vec![vec![Some("0".into())]], "not tables");
    let mut ctx = TxnContext::default();
    let out = batch(&engine, &mut ctx, "SELECT * FROM app");
    assert_eq!(
        out.error.as_ref().map(|e| e.number),
        Some(208),
        "a user name is not a queryable object"
    );

    // Survives restart.
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT name FROM sys.database_principals WHERE name = 'reporting'",
    );
    assert_eq!(rows, vec![vec![Some("reporting".into())]], "role survives");

    // DROP.
    engine.execute("DROP ROLE reporting").expect("drop role");
    engine.execute("DROP USER app").expect("drop user");
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.database_principals \
             WHERE name IN ('app','reporting')",
    );
    assert_eq!(rows, vec![vec![Some("0".into())]], "dropped");
    let _ = std::fs::remove_file(path);
}

#[test]
fn role_membership_is_transitive_and_cycle_checked() {
    let path = unique_temp_path("membership");
    let engine = new_engine(&path);
    engine.execute("CREATE ROLE r1").unwrap();
    engine.execute("CREATE ROLE r2").unwrap();
    engine.execute("CREATE USER u").unwrap();
    // u ∈ r1, r1 ∈ r2 (nesting).
    engine.execute("ALTER ROLE r1 ADD MEMBER u").unwrap();
    engine.execute("ALTER ROLE r2 ADD MEMBER r1").unwrap();

    // sys.database_role_members: the two edges plus the synthesized dbo→db_owner.
    let (_c, rows) = sql_rows(
        &engine,
        "SELECT COUNT(*) AS n FROM sys.database_role_members",
    );
    assert_eq!(rows, vec![vec![Some("3".into())]]);

    // A cycle (r2 → r1 → r2) is refused, as is self-membership.
    let mut ctx = TxnContext::default();
    assert!(
        batch(&engine, &mut ctx, "ALTER ROLE r1 ADD MEMBER r2")
            .error
            .is_some(),
        "a membership cycle must be rejected"
    );
    assert!(
        batch(&engine, &mut ctx, "ALTER ROLE r1 ADD MEMBER r1")
            .error
            .is_some(),
        "self-membership must be rejected"
    );

    // A role with members cannot be dropped.
    assert!(
        batch(&engine, &mut ctx, "DROP ROLE r1").error.is_some(),
        "a role with members cannot be dropped"
    );
    // Remove the members, then it drops.
    engine.execute("ALTER ROLE r1 DROP MEMBER u").unwrap();
    engine.execute("ALTER ROLE r2 DROP MEMBER r1").unwrap();
    engine.execute("DROP ROLE r1").expect("now droppable");
    let _ = std::fs::remove_file(path);
}

#[test]
fn session_intrinsics_resolve_database_user_and_role_membership() {
    let path = unique_temp_path("intrinsics");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .unwrap();
    engine.execute("CREATE ROLE reporting").unwrap();
    engine.execute("CREATE USER analyst").unwrap();
    engine
        .execute("ALTER ROLE reporting ADD MEMBER analyst")
        .unwrap();

    // Helper: read the first row of a batch's first rowset as strings.
    fn row_cells(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> Vec<Option<String>> {
        let out = batch(engine, ctx, sql);
        assert!(out.error.is_none(), "batch error: {:?}", out.error);
        for result in &out.results {
            if let StatementResult::Rows(rowset) = result {
                return rowset.rows[0]
                    .iter()
                    .map(|d| match d {
                        Datum::Null => None,
                        Datum::Int(v) => Some(v.to_string()),
                        Datum::BigInt(v) => Some(v.to_string()),
                        Datum::NVarChar(s) | Datum::VarChar(s) => Some(s.clone()),
                        other => Some(format!("{other:?}")),
                    })
                    .collect();
            }
        }
        panic!("no rowset: {:?}", out.results);
    }

    // A session as sa maps to the dbo user (sysadmin), a member of db_owner.
    let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
    let (user, user_sid) = engine.resolve_session_user("sa", sa_sid);
    assert_eq!(user, "dbo");
    let mut ctx = TxnContext::default();
    ctx.set_session_identity(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "sa".into(),
        1,
        user,
        sa_sid,
        user_sid,
    );
    let cells = row_cells(
        &engine,
        &mut ctx,
        "SELECT SUSER_SNAME() a, USER_NAME() b, IS_SRVROLEMEMBER('sysadmin') c, \
             IS_ROLEMEMBER('db_owner') d, IS_ROLEMEMBER('reporting') e, \
             IS_ROLEMEMBER('sysadmin') f, IS_SRVROLEMEMBER('db_owner') g",
    );
    assert_eq!(
        cells,
        vec![
            Some("sa".into()),
            Some("dbo".into()),
            Some("1".into()), // IS_SRVROLEMEMBER(sysadmin)
            Some("1".into()), // IS_ROLEMEMBER(db_owner)
            Some("0".into()), // IS_ROLEMEMBER(reporting)
            // The role families do not cross-answer: sysadmin is a SERVER
            // role (0 as a database role), db_owner a DATABASE role (0 as a
            // server role).
            Some("0".into()), // IS_ROLEMEMBER(sysadmin)
            Some("0".into()), // IS_SRVROLEMEMBER(db_owner)
        ],
        "sa → dbo; server/database role namespaces are distinct"
    );

    // A session as the analyst user is a member of reporting only.
    let analyst_sid: u32 = {
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT principal_id FROM sys.database_principals WHERE name = 'analyst'",
        );
        rows[0][0].as_ref().unwrap().parse().unwrap()
    };
    let mut ctx = TxnContext::default();
    ctx.set_session_identity(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "analyst".into(),
        2,
        "analyst".into(),
        0,
        analyst_sid,
    );
    let cells = row_cells(
        &engine,
        &mut ctx,
        "SELECT USER_NAME() a, IS_ROLEMEMBER('reporting') b, IS_ROLEMEMBER('db_owner') c, \
             IS_SRVROLEMEMBER('sysadmin') d",
    );
    assert_eq!(
        cells,
        vec![
            Some("analyst".into()),
            Some("1".into()),
            Some("0".into()),
            Some("0".into()),
        ],
        "analyst ∈ reporting only"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sa_resolves_to_dbo_and_sysadmin_with_no_prior_security_ddl() {
    // Regression: the membership cache must populate on the very first query
    // even while security_version is still 0 (a fresh boot where only the sa
    // login was created), and again after a restart resets the counter to 0.
    let path = unique_temp_path("fresh-sa");
    let engine = new_engine(&path);
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .unwrap();
    // No CREATE USER/ROLE/ALTER ROLE has run: security_version is still 0.
    let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
    assert_eq!(
        engine.resolve_session_user("sa", sa_sid),
        ("dbo".to_string(), crate::storage::DBO_ID),
        "sa is sysadmin (→ dbo) with no prior security DDL"
    );

    // A durable role membership survives a restart (which resets the in-memory
    // counter and cache) and is visible immediately, before any new DDL.
    engine.execute("CREATE ROLE r").unwrap();
    engine.execute("CREATE USER u").unwrap();
    engine.execute("ALTER ROLE r ADD MEMBER u").unwrap();
    let u_sid: u32 = {
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT principal_id FROM sys.database_principals WHERE name = 'u'",
        );
        rows[0][0].as_ref().unwrap().parse().unwrap()
    };
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
    let r_sid: u32 = {
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT principal_id FROM sys.database_principals WHERE name = 'r'",
        );
        rows[0][0].as_ref().unwrap().parse().unwrap()
    };
    assert!(
        engine
            .storage_effective_roles_for_test(u_sid)
            .contains(&r_sid),
        "durable membership is visible after restart with no new DDL"
    );
    // And sa still resolves to sysadmin/dbo post-restart.
    let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
    assert_eq!(
        engine.resolve_session_user("sa", sa_sid).0,
        "dbo",
        "sa is still sysadmin after restart"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn creating_the_sa_login_against_a_warm_cache_is_seen() {
    // Regression: login DDL bumps the security version, so creating sa after
    // the membership cache is already warm still makes it sysadmin.
    let path = unique_temp_path("warm-sa");
    let engine = new_engine(&path);
    engine.execute("CREATE ROLE warm").unwrap(); // warms the cache at version >= 1
    // A resolve while sa is absent warms the cache without an sa edge.
    assert_eq!(
        engine.resolve_session_user("sa", 999),
        ("sa".to_string(), 0)
    );
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .unwrap();
    let sa_sid = engine.lookup_login("sa").unwrap().principal_id;
    assert_eq!(
        engine.resolve_session_user("sa", sa_sid),
        ("dbo".to_string(), crate::storage::DBO_ID),
        "the freshly-created sa is sysadmin despite the warm cache"
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn a_membership_change_is_visible_to_the_next_batch() {
    // The security-version counter invalidates the membership cache: a role
    // added between batches is reflected in the next batch's IS_ROLEMEMBER.
    let path = unique_temp_path("membership-invalidation");
    let engine = new_engine(&path);
    engine.execute("CREATE ROLE auditors").unwrap();
    engine.execute("CREATE USER clerk").unwrap();
    let clerk_sid: u32 = {
        let (_c, rows) = sql_rows(
            &engine,
            "SELECT principal_id FROM sys.database_principals WHERE name = 'clerk'",
        );
        rows[0][0].as_ref().unwrap().parse().unwrap()
    };
    let mut ctx = TxnContext::default();
    ctx.set_session_identity(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        "clerk".into(),
        3,
        "clerk".into(),
        0,
        clerk_sid,
    );

    let member = |engine: &Engine, ctx: &mut TxnContext| -> i64 {
        let out = batch(engine, ctx, "SELECT IS_ROLEMEMBER('auditors')");
        match &out.results[0] {
            StatementResult::Rows(rowset) => match rowset.rows[0][0] {
                Datum::Int(v) => v as i64,
                Datum::BigInt(v) => v,
                ref other => panic!("expected an integer, got {other:?}"),
            },
            other => panic!("expected rows, got {other:?}"),
        }
    };

    assert_eq!(member(&engine, &mut ctx), 0, "not a member yet");
    engine
        .execute("ALTER ROLE auditors ADD MEMBER clerk")
        .unwrap();
    assert_eq!(
        member(&engine, &mut ctx),
        1,
        "the new membership is seen after the security-version bump"
    );
    let _ = std::fs::remove_file(path);
}

/// Opens a restricted (non-dbo, non-sysadmin) session context for `login`,
/// so object-permission checks actually bite (unlike the login_sid-0 bypass).
fn restricted_ctx(engine: &Engine, login: &str) -> TxnContext {
    let login_sid = engine.lookup_login(login).unwrap().principal_id;
    let (user, user_sid) = engine.resolve_session_user(login, login_sid);
    let mut ctx = TxnContext::default();
    ctx.set_session_identity(
        "truthdb".into(),
        crate::relstore::catalog::DEFAULT_DATABASE_ID,
        login.into(),
        9,
        user,
        login_sid,
        user_sid,
    );
    ctx
}

fn err_num(engine: &Engine, ctx: &mut TxnContext, sql: &str) -> Option<i32> {
    batch(engine, ctx, sql).error.as_ref().map(|e| e.number)
}

#[test]
fn object_permissions_enforce_grant_deny_revoke_and_public() {
    let path = unique_temp_path("perms");
    let engine = new_engine(&path);
    // Admin (login_sid 0 → bypass) sets up objects, a restricted login/user,
    // and a role.
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    engine
        .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
        .unwrap();
    engine
        .execute("CREATE USER appuser FOR LOGIN applogin")
        .unwrap();
    engine.execute("CREATE ROLE readers").unwrap();
    engine
        .execute("ALTER ROLE readers ADD MEMBER appuser")
        .unwrap();

    let mut r = restricted_ctx(&engine, "applogin");

    // Ungranted: SELECT denied 229.
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
    // GRANT SELECT via the role → allowed.
    engine.execute("GRANT SELECT ON t TO readers").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
    // A direct DENY beats the role's GRANT (both entries present).
    engine.execute("DENY SELECT ON t TO appuser").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
    // REVOKE the deny → the role GRANT is effective again.
    engine.execute("REVOKE SELECT ON t FROM appuser").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
    // REVOKE from the role → no grant → denied.
    engine.execute("REVOKE SELECT ON t FROM readers").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));

    // GRANT to public covers every user.
    engine.execute("GRANT SELECT ON t TO public").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);

    // INSERT/UPDATE/DELETE need their own grants.
    assert_eq!(
        err_num(&engine, &mut r, "INSERT INTO t VALUES (3)"),
        Some(229)
    );
    engine.execute("GRANT INSERT ON t TO appuser").unwrap();
    assert_eq!(err_num(&engine, &mut r, "INSERT INTO t VALUES (3)"), None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn execute_permission_and_ownership_chaining() {
    let path = unique_temp_path("perms-chain");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE secret (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO secret VALUES (42)").unwrap();
    engine
        .execute("CREATE PROCEDURE read_secret AS SELECT id FROM secret")
        .unwrap();
    engine
        .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
        .unwrap();
    engine
        .execute("CREATE USER appuser FOR LOGIN applogin")
        .unwrap();

    let mut r = restricted_ctx(&engine, "applogin");
    // No EXECUTE grant: EXEC denied.
    assert_eq!(err_num(&engine, &mut r, "EXEC read_secret"), Some(229));
    // GRANT EXECUTE only on the proc — NOT SELECT on the table it reads.
    engine
        .execute("GRANT EXECUTE ON read_secret TO appuser")
        .unwrap();
    // Ownership chaining: the proc runs, its body's SELECT on secret is not
    // re-checked (same dbo owner).
    assert_eq!(err_num(&engine, &mut r, "EXEC read_secret"), None);
    // But a DIRECT read of the table is still denied.
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM secret"), Some(229));

    // Dynamic SQL does NOT ownership-chain: a restricted user cannot escape
    // the check by wrapping the read (or a write) in sp_executesql.
    assert_eq!(
        err_num(
            &engine,
            &mut r,
            "EXEC sp_executesql N'SELECT id FROM secret'"
        ),
        Some(229)
    );
    assert_eq!(
        err_num(&engine, &mut r, "EXEC sp_executesql N'DELETE FROM secret'"),
        Some(229)
    );
    assert_eq!(
        err_num(
            &engine,
            &mut r,
            "EXEC sp_executesql N'INSERT INTO secret VALUES (7)'"
        ),
        Some(229)
    );
    // A GRANT makes the dynamic read work (proving the check, not a blanket ban).
    engine.execute("GRANT SELECT ON secret TO appuser").unwrap();
    assert_eq!(
        err_num(
            &engine,
            &mut r,
            "EXEC sp_executesql N'SELECT id FROM secret'"
        ),
        None
    );
    engine
        .execute("REVOKE SELECT ON secret FROM appuser")
        .unwrap();

    // Dynamic SQL nested INSIDE a procedure body still does not chain: the
    // dynamic read is checked as the caller (DynamicScope resets the chaining
    // depth). Grant EXECUTE on the wrapper proc but NOT SELECT on the table.
    engine
        .execute("CREATE PROCEDURE dyn_read AS EXEC sp_executesql N'SELECT id FROM secret'")
        .unwrap();
    engine
        .execute("GRANT EXECUTE ON dyn_read TO appuser")
        .unwrap();
    assert_eq!(err_num(&engine, &mut r, "EXEC dyn_read"), Some(229));
    // Granting the caller SELECT lets the nested dynamic read succeed.
    engine.execute("GRANT SELECT ON secret TO appuser").unwrap();
    assert_eq!(err_num(&engine, &mut r, "EXEC dyn_read"), None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn dropping_a_grantee_scrubs_its_object_permissions() {
    let path = unique_temp_path("perms-scrub");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE USER alice").unwrap();
    engine.execute("GRANT SELECT ON t TO alice").unwrap();

    let perm_count = |engine: &Engine| -> String {
        let (_c, rows) = sql_rows(
            engine,
            "SELECT COUNT(*) FROM sys.database_permissions WHERE major_id = \
                 (SELECT object_id FROM sys.tables WHERE name = 't')",
        );
        rows[0][0].clone().unwrap()
    };
    assert_eq!(perm_count(&engine), "1");
    // Dropping the grantee removes the dangling entry (so a later object_id
    // reuse after restart cannot re-point it at a new principal).
    engine.execute("DROP USER alice").unwrap();
    assert_eq!(perm_count(&engine), "0");
    // Persisted: the scrub survives a restart.
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
    assert_eq!(perm_count(&engine), "0");
    let _ = std::fs::remove_file(path);
}

// ---- Stage 16 exit matrices --------------------------------------------

#[test]
fn deny_beats_grant_across_nested_roles() {
    // A user in a nested role chain (u ∈ r1 ∈ r2): a GRANT high in the chain
    // is overridden by a DENY at any level, and REVOKE of the DENY restores
    // the inherited GRANT — the DENY/GRANT truth table across nested roles.
    let path = unique_temp_path("exit-deny");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO t VALUES (1)").unwrap();
    engine
        .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
        .unwrap();
    engine
        .execute("CREATE USER appuser FOR LOGIN applogin")
        .unwrap();
    engine.execute("CREATE ROLE r1").unwrap();
    engine.execute("CREATE ROLE r2").unwrap();
    engine.execute("ALTER ROLE r1 ADD MEMBER appuser").unwrap();
    engine.execute("ALTER ROLE r2 ADD MEMBER r1").unwrap(); // u ∈ r1 ∈ r2

    let mut r = restricted_ctx(&engine, "applogin");
    // Ungranted → denied.
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
    // GRANT to the OUTER role r2 is inherited transitively through r1.
    engine.execute("GRANT SELECT ON t TO r2").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
    // A DENY at the INTERMEDIATE role r1 beats the inherited grant.
    engine.execute("DENY SELECT ON t TO r1").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
    // REVOKE the DENY → the r2 grant is inherited again.
    engine.execute("REVOKE SELECT ON t FROM r1").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), None);
    // A DENY on the user directly also beats the role grant.
    engine.execute("DENY SELECT ON t TO appuser").unwrap();
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM t"), Some(229));
    let _ = std::fs::remove_file(path);
}

#[test]
fn ownership_chaining_through_a_proc_over_a_view_over_a_table() {
    // The classic chain: EXECUTE on a proc, which reads a view, which reads a
    // table — the restricted user needs neither SELECT on the view nor on the
    // table (all owned by dbo), only EXECUTE on the proc.
    let path = unique_temp_path("exit-chain");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE base (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("INSERT INTO base VALUES (7)").unwrap();
    engine
        .execute("CREATE VIEW v AS SELECT id FROM base")
        .unwrap();
    engine
        .execute("CREATE PROCEDURE p AS SELECT id FROM v")
        .unwrap();
    engine
        .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
        .unwrap();
    engine
        .execute("CREATE USER appuser FOR LOGIN applogin")
        .unwrap();
    engine.execute("GRANT EXECUTE ON p TO appuser").unwrap();

    let mut r = restricted_ctx(&engine, "applogin");
    // Runs via the chain — no SELECT on v or base needed.
    assert_eq!(err_num(&engine, &mut r, "EXEC p"), None);
    // Direct reads of the view and the table are still denied.
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM v"), Some(229));
    assert_eq!(err_num(&engine, &mut r, "SELECT id FROM base"), Some(229));
    let _ = std::fs::remove_file(path);
}

#[test]
fn config_migration_upgrades_an_existing_pre_stage16_catalog() {
    use std::collections::BTreeMap;
    let path = unique_temp_path("exit-migrate");
    // A "pre-Stage-16" database: it has schema objects but migration never
    // ran, so there are no catalog logins yet.
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    assert!(engine.lookup_login("sa").is_none(), "no logins yet");
    drop(engine);

    // Reopen under the new build and run first-boot migration with config
    // users — the existing catalog is upgraded in place.
    let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
    let mut users = BTreeMap::new();
    users.insert("sa".to_string(), "secret".to_string());
    users.insert("app".to_string(), "app-pw".to_string());
    let created = engine.migrate_logins(&users).expect("migrate");
    assert!(created.contains(&"app".to_string()), "config user migrated");
    assert!(engine.lookup_login("sa").is_some(), "sa seeded");
    assert!(engine.lookup_login("app").is_some(), "app seeded");
    // The pre-existing table is untouched.
    let (_c, rows) = sql_rows(&engine, "SELECT COUNT(*) FROM sys.tables WHERE name = 't'");
    assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    // Idempotent: a subsequent start does not re-migrate.
    assert!(engine.migrate_logins(&users).expect("second").is_empty());
    let _ = std::fs::remove_file(path);
}

#[test]
fn ddl_and_grant_require_privilege() {
    let path = unique_temp_path("perms-ddl");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine
        .execute("CREATE LOGIN applogin WITH PASSWORD = 'x'")
        .unwrap();
    engine
        .execute("CREATE USER appuser FOR LOGIN applogin")
        .unwrap();

    let mut r = restricted_ctx(&engine, "applogin");
    // A restricted user cannot run DDL...
    assert_eq!(
        err_num(
            &engine,
            &mut r,
            "CREATE TABLE hax (id INT NOT NULL PRIMARY KEY)"
        ),
        Some(15247)
    );
    assert_eq!(err_num(&engine, &mut r, "DROP TABLE t"), Some(15247));
    assert_eq!(err_num(&engine, &mut r, "CREATE ROLE sneaky"), Some(15247));
    // ...and cannot grant itself permissions.
    assert_eq!(
        err_num(&engine, &mut r, "GRANT SELECT ON t TO appuser"),
        Some(15247)
    );
    // A sysadmin (sa) session bypasses — DDL and GRANT succeed.
    engine
        .execute("CREATE LOGIN sa WITH PASSWORD = 'x'")
        .unwrap();
    let mut admin = restricted_ctx(&engine, "sa"); // sa → dbo/sysadmin → bypass
    assert_eq!(
        err_num(&engine, &mut admin, "GRANT SELECT ON t TO appuser"),
        None
    );
    assert_eq!(
        err_num(
            &engine,
            &mut admin,
            "CREATE TABLE ok (id INT NOT NULL PRIMARY KEY)"
        ),
        None
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn sys_database_permissions_reflects_grants_and_survives_restart() {
    let path = unique_temp_path("perms-catalog");
    let engine = new_engine(&path);
    engine
        .execute("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
        .unwrap();
    engine.execute("CREATE USER appuser").unwrap();
    engine.execute("GRANT SELECT ON t TO appuser").unwrap();
    engine.execute("DENY UPDATE ON t TO appuser").unwrap();

    let check = |engine: &Engine| {
        let (_c, rows) = sql_rows(
            engine,
            "SELECT permission_name, state_desc FROM sys.database_permissions \
                 WHERE major_id = (SELECT object_id FROM sys.tables WHERE name = 't') \
                 ORDER BY permission_name",
        );
        assert_eq!(
            rows,
            vec![
                vec![Some("SELECT".into()), Some("GRANT".into())],
                vec![Some("UPDATE".into()), Some("DENY".into())],
            ]
        );
    };
    check(&engine);
    // Survives restart (permissions ride the object's catalog row).
    drop(engine);
    let engine = Engine::new(Storage::open(path.clone()).expect("reopen")).expect("replay");
    check(&engine);
    let _ = std::fs::remove_file(path);
}
