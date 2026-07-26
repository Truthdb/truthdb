// Fixed (built-in) principals are SYNTHESIZED at read time, never stored. Their
// principal_ids sit in a reserved band far above any real `object_id` (which
// starts at FIRST_USER_OBJECT_ID=2 and increments by one), so they can never
// collide with a created table, index, login, user, or role.
pub(crate) const FIXED_PRINCIPAL_BASE: u32 = 0x7000_0000;
pub(crate) const DBO_ID: u32 = FIXED_PRINCIPAL_BASE; // the dbo database user
pub(crate) const SYSADMIN_ID: u32 = FIXED_PRINCIPAL_BASE + 1; // server role, bypass-all
pub(crate) const DB_OWNER_ID: u32 = FIXED_PRINCIPAL_BASE + 2;
pub(crate) const DB_DATAREADER_ID: u32 = FIXED_PRINCIPAL_BASE + 3;
pub(crate) const DB_DATAWRITER_ID: u32 = FIXED_PRINCIPAL_BASE + 4;
pub(crate) const DB_DDLADMIN_ID: u32 = FIXED_PRINCIPAL_BASE + 5;
pub(crate) const PUBLIC_ID: u32 = FIXED_PRINCIPAL_BASE + 6; // every user is a member

/// A built-in principal, synthesized (not stored) into `sys.database_principals`,
/// name resolution for membership DDL, and the membership edge set.
pub(crate) struct FixedPrincipal {
    pub id: u32,
    pub name: &'static str,
    pub kind: crate::relstore::catalog::PrincipalKind,
    /// True for the sysadmin SERVER role (a login's role), false for database
    /// roles/users (a user's role). Governs which view/intrinsic family sees it.
    pub is_server: bool,
}

/// The built-in principals: the dbo user, the sysadmin server role, and the
/// fixed database roles. `public` is implicit (every database principal belongs
/// to it) and is listed so it appears in the catalog view.
pub(crate) const FIXED_PRINCIPALS: &[FixedPrincipal] = {
    use crate::relstore::catalog::PrincipalKind::{Role, User};
    &[
        FixedPrincipal {
            id: DBO_ID,
            name: "dbo",
            kind: User,
            is_server: false,
        },
        FixedPrincipal {
            id: SYSADMIN_ID,
            name: "sysadmin",
            kind: Role,
            is_server: true,
        },
        FixedPrincipal {
            id: DB_OWNER_ID,
            name: "db_owner",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DATAREADER_ID,
            name: "db_datareader",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DATAWRITER_ID,
            name: "db_datawriter",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: DB_DDLADMIN_ID,
            name: "db_ddladmin",
            kind: Role,
            is_server: false,
        },
        FixedPrincipal {
            id: PUBLIC_ID,
            name: "public",
            kind: Role,
            is_server: false,
        },
    ]
};

/// The fixed principal with this (case-insensitive) name, if any.
pub(crate) fn fixed_principal_by_name(name: &str) -> Option<&'static FixedPrincipal> {
    FIXED_PRINCIPALS
        .iter()
        .find(|p| p.name.eq_ignore_ascii_case(name))
}

/// The fixed principal with this id, if any.
pub(crate) fn fixed_principal_by_id(id: u32) -> Option<&'static FixedPrincipal> {
    FIXED_PRINCIPALS.iter().find(|p| p.id == id)
}

/// An empty-schema catalog row carrying a principal payload (login / user / role).
fn principal_table_def(
    object_id: u32,
    name: String,
    principal: crate::relstore::catalog::PrincipalDef,
) -> TableDef {
    TableDef {
        object_id,
        name,
        columns: Vec::new(),
        key_columns: Vec::new(),
        root_page: 0,
        defaults: Vec::new(),
        collations: Vec::new(),
        identity: None,
        indexes: Vec::new(),
        check_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        view_query: None,
        procedure: None,
        function: None,
        trigger: None,
        principal: Some(principal),
        permissions: Vec::new(),
        counter_page: None,
        database_id: catalog::DEFAULT_DATABASE_ID,
        database: None,
    }
}

use super::super::*;

impl Storage {
    // Logins do not participate in lock analysis, so — unlike table/proc DDL —
    // they do not bump the lock-analysis epoch. They DO bump the security
    // version: which login is named `sa` decides the synthesized sa→sysadmin
    // membership edge, so creating/dropping/re-keying a login must invalidate the
    // membership cache.
    pub fn rel_create_login(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_login(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_alter_login(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_alter_login(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_login(&self, name: &str) -> Result<bool, StorageError> {
        let dropped = self.lock().rel_drop_login(name)?;
        if dropped {
            self.bump_security_version();
        }
        Ok(dropped)
    }

    pub fn rel_login(&self, name: &str) -> Option<TableDef> {
        self.lock().rel_login(name)
    }

    pub fn rel_logins(&self) -> Vec<TableDef> {
        self.lock().rel_logins()
    }

    // Database-principal / role DDL bumps the security version (invalidating the
    // membership cache), NOT the lock epoch — authorization changes no lock set.
    pub fn rel_create_database_principal(
        &self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.lock().rel_create_database_principal(name, principal)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_database_principal(&self, name: &str) -> Result<bool, StorageError> {
        let dropped = self.lock().rel_drop_database_principal(name)?;
        if dropped {
            self.bump_security_version();
        }
        Ok(dropped)
    }

    pub fn rel_add_role_member(&self, role: &str, member: &str) -> Result<(), StorageError> {
        self.lock().rel_add_role_member(role, member)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_drop_role_member(&self, role: &str, member: &str) -> Result<(), StorageError> {
        self.lock().rel_drop_role_member(role, member)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_database_principal(&self, name: &str) -> Option<TableDef> {
        self.lock().rel_database_principal(name)
    }

    pub fn rel_database_principals(&self) -> Vec<TableDef> {
        self.lock().rel_database_principals()
    }

    // GRANT/DENY/REVOKE bump the security version so a per-batch security context
    // is recomputed next batch (like membership DDL).
    pub fn rel_grant_object(
        &self,
        db_id: u32,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
        deny: bool,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_grant_object(db_id, object, grantee, action, deny)?;
        self.bump_security_version();
        Ok(())
    }

    pub fn rel_revoke_object(
        &self,
        db_id: u32,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
    ) -> Result<(), StorageError> {
        self.lock()
            .rel_revoke_object(db_id, object, grantee, action)?;
        self.bump_security_version();
        Ok(())
    }

    /// The name of the principal with this id — a fixed principal, a login, or a
    /// database user/role. `None` for an unknown id.
    pub(crate) fn principal_name(&self, id: u32) -> Option<String> {
        if let Some(fixed) = fixed_principal_by_id(id) {
            return Some(fixed.name.to_string());
        }
        let guard = self.lock();
        guard
            .rel
            .principals
            .values()
            .chain(guard.rel.database_principals.values())
            .find(|d| d.object_id == id)
            .map(|d| d.name.clone())
    }

    /// Bumps the security version after a security DDL commits. Release-ordered
    /// so a reader that observes the new version also observes the new catalog
    /// rows (the bump happens after the inner mutation returns).
    pub(in crate::storage) fn bump_security_version(&self) {
        self.security_version
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The security version: the membership cache tagged with an older value is
    /// discarded and recomputed on the next query.
    pub(crate) fn security_version(&self) -> u64 {
        self.security_version
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The effective (transitively-closed) set of role principal_ids a principal
    /// belongs to, memoized and invalidated by [`Self::security_version`]. Does
    /// NOT include the principal itself. Cycle-safe (visited-set DFS), so a role
    /// graph that somehow contains a cycle terminates rather than hanging.
    pub(crate) fn effective_roles(&self, principal_id: u32) -> std::collections::HashSet<u32> {
        let version = self.security_version();
        // Fast path: a cache hit under the current version needs only the
        // membership lock.
        {
            let cache = self.membership.lock().expect("membership cache poisoned");
            if cache.version == Some(version)
                && let Some(hit) = cache.closure.get(&principal_id)
            {
                return hit.clone();
            }
        }
        // Slow path: (re)build. The edge set is collected WITHOUT the membership
        // lock held (it takes the storage lock) so the two locks are never nested
        // — collect first, then lock the cache. A version move between the read
        // and here just costs one recompute, corrected on the next call, exactly
        // like the lock-epoch discipline.
        let edges = self.collect_membership_edges();
        let mut cache = self.membership.lock().expect("membership cache poisoned");
        if cache.version != Some(version) {
            cache.version = Some(version);
            cache.closure.clear();
            cache.edges = edges;
        }
        if let Some(hit) = cache.closure.get(&principal_id) {
            return hit.clone();
        }
        let mut result = std::collections::HashSet::new();
        let mut stack = cache.edges.get(&principal_id).cloned().unwrap_or_default();
        while let Some(role) = stack.pop() {
            if result.insert(role)
                && let Some(parents) = cache.edges.get(&role)
            {
                stack.extend(parents.iter().copied());
            }
        }
        cache.closure.insert(principal_id, result.clone());
        result
    }

    /// The raw membership edge set `principal_id -> direct role principal_ids`,
    /// unioning stored `member_of` edges (logins + database principals) with the
    /// synthesized fixed-principal bootstrap edges (`sa -> sysadmin`,
    /// `dbo -> db_owner`).
    pub(in crate::storage) fn collect_membership_edges(
        &self,
    ) -> std::collections::HashMap<u32, Vec<u32>> {
        let guard = self.lock();
        let mut edges: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
        for def in guard
            .rel
            .principals
            .values()
            .chain(guard.rel.database_principals.values())
        {
            if let Some(principal) = &def.principal
                && !principal.member_of.is_empty()
            {
                edges
                    .entry(def.object_id)
                    .or_default()
                    .extend(principal.member_of.iter().copied());
            }
        }
        // Synthesized bootstrap: whichever login is `sa` is a member of the
        // sysadmin server role, and the dbo user is a member of db_owner.
        if let Some(sa) = guard.rel.principals.get("sa") {
            edges.entry(sa.object_id).or_default().push(SYSADMIN_ID);
        }
        edges.entry(DBO_ID).or_default().push(DB_OWNER_ID);
        edges
    }
}

impl StorageFile {
    /// Creates a server login: a catalog row (its own object_id, like a
    /// procedure) carrying the hashed password, routed into the principals map
    /// so it never enters the object namespace.
    pub fn rel_create_login(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let key = name.to_ascii_lowercase();
        if self.rel.principals.contains_key(&key) {
            return Err(StorageError::Constraint(format!(
                "login '{name}' already exists"
            )));
        }
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let login_name = name.to_string();
        let def = self.rel_statement(move |ctx, txn| {
            let def = TableDef {
                object_id,
                name: login_name,
                columns: Vec::new(),
                key_columns: Vec::new(),
                root_page: 0,
                defaults: Vec::new(),
                collations: Vec::new(),
                identity: None,
                indexes: Vec::new(),
                check_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                view_query: None,
                procedure: None,
                function: None,
                trigger: None,
                principal: Some(principal),
                permissions: Vec::new(),
                counter_page: None,
                database_id: catalog::DEFAULT_DATABASE_ID,
                database: None,
            };
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &def)?;
            Ok(def)
        })?;
        self.rel.next_object_id += 1;
        self.rel.principals.insert(key, def);
        Ok(())
    }

    /// Replaces a login's payload (`ALTER LOGIN` — new password or enable/
    /// disable). The name (and object_id) is preserved.
    pub fn rel_alter_login(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let key = name.to_ascii_lowercase();
        let Some(existing) = self.rel.principals.get(&key) else {
            return Err(StorageError::Constraint(format!(
                "login '{name}' does not exist"
            )));
        };
        let mut def = existing.clone();
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("logins live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.principals.insert(key, def);
        Ok(())
    }

    /// Drops a login. Returns false if it does not exist.
    pub fn rel_drop_login(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let key = name.to_ascii_lowercase();
        let Some(def) = self.rel.principals.get(&key).cloned() else {
            return Ok(false);
        };
        let Some(catalog_root) = self.rel.catalog_root else {
            return Ok(false);
        };
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.principals.remove(&key);
        Ok(true)
    }

    /// A login's definition, by (case-insensitive) name.
    pub fn rel_login(&self, name: &str) -> Option<TableDef> {
        self.rel.principals.get(&name.to_ascii_lowercase()).cloned()
    }

    /// All logins, ordered by object id (for sys.server_principals).
    pub fn rel_logins(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.principals.values().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// Creates a database user or role (an empty-schema row carrying the given
    /// principal payload), routed into the `database_principals` map. Rejects a
    /// name already taken by a user/role or a fixed principal.
    pub fn rel_create_database_principal(
        &mut self,
        name: &str,
        principal: crate::relstore::catalog::PrincipalDef,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let key = name.to_ascii_lowercase();
        if self.rel.database_principals.contains_key(&key)
            || fixed_principal_by_name(&key).is_some()
        {
            return Err(StorageError::Constraint(format!(
                "principal '{name}' already exists"
            )));
        }
        if self.rel.catalog_root.is_none() {
            let root = {
                let mut ctx = self.rel_ctx();
                catalog::create_catalog(&mut ctx)?
            };
            self.rel.catalog_root = Some(root);
        }
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let object_id = self.rel.next_object_id;
        let def = principal_table_def(object_id, name.to_string(), principal);
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::insert_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.next_object_id += 1;
        self.rel.database_principals.insert(key, def);
        Ok(())
    }

    /// Drops a database user or role. A role that still has members is refused
    /// (dropping it would dangle their `member_of` edges — SQL Server 15144).
    /// Returns false if no such principal exists.
    pub fn rel_drop_database_principal(&mut self, name: &str) -> Result<bool, StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let key = name.to_ascii_lowercase();
        let Some(def) = self.rel.database_principals.get(&key).cloned() else {
            return Ok(false);
        };
        let role_id = def.object_id;
        let is_role = matches!(
            def.principal.as_ref().map(|p| p.kind),
            Some(crate::relstore::catalog::PrincipalKind::Role)
        );
        if is_role && self.principal_has_members(role_id) {
            return Err(StorageError::Constraint(format!(
                "the role '{name}' has members and cannot be dropped"
            )));
        }
        let catalog_root = self
            .rel
            .catalog_root
            .expect("database principals live in the catalog");
        // Scrub the principal's object permission entries BEFORE deleting it.
        // Object_ids can be reused after a restart (next_object_id is recomputed
        // from the surviving max), so a dangling grantee id must never outlive
        // its principal. These are separate WAL transactions; doing the scrub
        // first means a crash mid-drop can leave a still-present principal with
        // fewer grants (harmless, and a re-run finishes) but never the dangerous
        // deleted-principal-with-dangling-grant state.
        self.scrub_grants_for(role_id)?;
        self.rel_statement(move |ctx, txn| {
            catalog::delete_table(ctx, &mut OpMode::Txn(txn), catalog_root, def.object_id)
        })?;
        self.rel.database_principals.remove(&key);
        Ok(true)
    }

    /// Removes every object permission entry whose grantee is `grantee` (a
    /// dropped principal), rewriting each affected object's catalog row.
    pub(in crate::storage) fn scrub_grants_for(
        &mut self,
        grantee: u32,
    ) -> Result<(), StorageError> {
        let affected: Vec<TableDef> = self
            .rel
            .all_tables()
            .filter(|def| def.permissions.iter().any(|p| p.grantee == grantee))
            .cloned()
            .collect();
        for mut def in affected {
            def.permissions.retain(|p| p.grantee != grantee);
            self.persist_object_permissions(def)?;
        }
        // The per-object rewrites latched each object's database; the caller
        // is a server-scoped principal drop whose remaining appends must be
        // tagged 0 — restore it.
        self.current_container = 0;
        Ok(())
    }

    /// Adds `member` (a user or role) to `role` (a database or fixed role).
    /// Rejects: an unknown role/member, a non-role target, self-membership, and
    /// any addition that would close a cycle in the role graph. Idempotent if the
    /// edge already exists.
    pub fn rel_add_role_member(&mut self, role: &str, member: &str) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let role_id = self
            .resolve_db_principal_id(role)
            .ok_or_else(|| StorageError::Constraint(format!("the role '{role}' does not exist")))?;
        if !self.is_role_id(role_id) {
            return Err(StorageError::Constraint(format!("'{role}' is not a role")));
        }
        let member_key = member.to_ascii_lowercase();
        let Some(existing) = self.rel.database_principals.get(&member_key).cloned() else {
            return Err(StorageError::Constraint(format!(
                "the member '{member}' does not exist"
            )));
        };
        let member_id = existing.object_id;
        if member_id == role_id {
            return Err(StorageError::Constraint(
                "a role cannot be a member of itself".into(),
            ));
        }
        let mut principal = existing
            .principal
            .clone()
            .expect("database principal has a payload");
        if principal.member_of.contains(&role_id) {
            return Ok(()); // already a member
        }
        // A cycle would form iff `role` can already reach `member` by following
        // membership (member is an ancestor of role).
        if self.reachable_roles(role_id).contains(&member_id) {
            return Err(StorageError::Constraint(format!(
                "adding '{member}' to role '{role}' would create a membership cycle"
            )));
        }
        principal.member_of.push(role_id);
        let mut def = existing;
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.database_principals.insert(member_key, def);
        Ok(())
    }

    /// Removes `member` from `role`. Idempotent (no error if not a member); errors
    /// only on an unknown role or member.
    pub fn rel_drop_role_member(&mut self, role: &str, member: &str) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = 0;
        let role_id = self
            .resolve_db_principal_id(role)
            .ok_or_else(|| StorageError::Constraint(format!("the role '{role}' does not exist")))?;
        let member_key = member.to_ascii_lowercase();
        let Some(existing) = self.rel.database_principals.get(&member_key).cloned() else {
            return Err(StorageError::Constraint(format!(
                "the member '{member}' does not exist"
            )));
        };
        let mut principal = existing
            .principal
            .clone()
            .expect("database principal has a payload");
        if !principal.member_of.contains(&role_id) {
            return Ok(()); // not a member
        }
        principal.member_of.retain(|&r| r != role_id);
        let mut def = existing;
        def.principal = Some(principal);
        let catalog_root = self.rel.catalog_root.expect("catalog exists");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.database_principals.insert(member_key, def);
        Ok(())
    }

    /// A database user/role definition, by (case-insensitive) name.
    pub fn rel_database_principal(&self, name: &str) -> Option<TableDef> {
        self.rel
            .database_principals
            .get(&name.to_ascii_lowercase())
            .cloned()
    }

    /// All stored database users and roles, ordered by object id (fixed
    /// principals are synthesized by the caller, not stored here).
    pub fn rel_database_principals(&self) -> Vec<TableDef> {
        let mut defs: Vec<TableDef> = self.rel.database_principals.values().cloned().collect();
        defs.sort_by_key(|d| d.object_id);
        defs
    }

    /// Resolves a database-scoped principal NAME (a stored user/role or a fixed
    /// database principal — dbo, db_owner, …, public; NOT the server sysadmin
    /// role) to its principal_id.
    pub(in crate::storage) fn resolve_db_principal_id(&self, name: &str) -> Option<u32> {
        if let Some(def) = self.rel_database_principal(name) {
            return Some(def.object_id);
        }
        fixed_principal_by_name(name)
            .filter(|p| !p.is_server)
            .map(|p| p.id)
    }

    /// True if the id is a role (a stored role or a fixed database role).
    pub(in crate::storage) fn is_role_id(&self, id: u32) -> bool {
        if let Some(p) = fixed_principal_by_id(id) {
            return matches!(p.kind, crate::relstore::catalog::PrincipalKind::Role);
        }
        self.rel
            .database_principals
            .values()
            .any(|d| d.object_id == id && d.is_role())
    }

    /// GRANT or DENY an action on an object to a grantee (a database user/role or
    /// a fixed database principal, incl. `public`). Replaces any existing entry
    /// for the same (grantee, action) — a GRANT and a DENY of the same action to
    /// the same grantee do not coexist. Errors on an unknown grantee or object.
    pub fn rel_grant_object(
        &mut self,
        db_id: u32,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
        deny: bool,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let grantee_id = self.resolve_grantee_id(grantee)?;
        let Some(mut def) = self.rel.table(db_id, object).cloned() else {
            return Err(StorageError::Constraint(format!(
                "cannot find the object '{object}', because it does not exist or you do not have permission"
            )));
        };
        def.permissions
            .retain(|p| !(p.grantee == grantee_id && p.action == action));
        def.permissions
            .push(crate::relstore::catalog::PermissionEntry {
                grantee: grantee_id,
                action,
                deny,
            });
        self.persist_object_permissions(def)
    }

    /// REVOKE an action on an object from a grantee — removes both the GRANT and
    /// the DENY of that (grantee, action). Idempotent (no error if absent).
    pub fn rel_revoke_object(
        &mut self,
        db_id: u32,
        object: &str,
        grantee: &str,
        action: crate::relstore::catalog::PermAction,
    ) -> Result<(), StorageError> {
        self.ensure_rel_usable()?;
        self.current_container = db_id as u16;
        let grantee_id = self.resolve_grantee_id(grantee)?;
        let Some(mut def) = self.rel.table(db_id, object).cloned() else {
            return Err(StorageError::Constraint(format!(
                "cannot find the object '{object}', because it does not exist or you do not have permission"
            )));
        };
        let before = def.permissions.len();
        def.permissions
            .retain(|p| !(p.grantee == grantee_id && p.action == action));
        if def.permissions.len() == before {
            return Ok(()); // nothing to revoke
        }
        self.persist_object_permissions(def)
    }

    /// Resolves a grantee NAME to a database principal_id (a stored user/role or
    /// a fixed database principal, incl. `public`; NOT the server sysadmin role).
    pub(in crate::storage) fn resolve_grantee_id(
        &self,
        grantee: &str,
    ) -> Result<u32, StorageError> {
        self.resolve_db_principal_id(grantee).ok_or_else(|| {
            StorageError::Constraint(format!(
                "cannot find the principal '{grantee}', because it does not exist"
            ))
        })
    }

    /// Writes an object's mutated permission list back through the catalog and
    /// the in-memory cache (one whole-row rewrite, like a role-member edit).
    pub(in crate::storage) fn persist_object_permissions(
        &mut self,
        def: TableDef,
    ) -> Result<(), StorageError> {
        self.current_container = def.database_id as u16;
        let catalog_root = self.rel.catalog_root.expect("objects live in the catalog");
        let write = def.clone();
        self.rel_statement(move |ctx, txn| {
            catalog::update_table(ctx, &mut OpMode::Txn(txn), catalog_root, &write)?;
            Ok(())
        })?;
        self.rel.cache_table(def);
        Ok(())
    }

    /// True if any principal (login or database) is a direct member of `role_id`.
    pub(in crate::storage) fn principal_has_members(&self, role_id: u32) -> bool {
        self.rel
            .database_principals
            .values()
            .chain(self.rel.principals.values())
            .any(|d| {
                d.principal
                    .as_ref()
                    .is_some_and(|p| p.member_of.contains(&role_id))
            })
    }

    /// The transitive ancestors of `start` (the roles it belongs to, directly or
    /// indirectly), over stored `member_of` edges. Visited-set guarded, so a
    /// pre-existing cycle terminates. Used to detect a would-be cycle before
    /// adding an edge.
    pub(in crate::storage) fn reachable_roles(&self, start: u32) -> std::collections::HashSet<u32> {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![start];
        while let Some(id) = stack.pop() {
            let parents = self
                .rel
                .database_principals
                .values()
                .chain(self.rel.principals.values())
                .find(|d| d.object_id == id)
                .and_then(|d| d.principal.as_ref())
                .map(|p| p.member_of.clone())
                .unwrap_or_default();
            for r in parents {
                if seen.insert(r) {
                    stack.push(r);
                }
            }
        }
        seen
    }
}
