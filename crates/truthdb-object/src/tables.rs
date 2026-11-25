//! S3 Tables' catalog: an account's table buckets, their namespaces and
//! their tables (each with its id, its warehouse location and the pointer
//! to its current Iceberg metadata file, changed by compare-and-swap of a
//! version token), and the policies of table buckets and tables.

use serde_json::{Value, json};
use truthdb_core::relstore::btree::TreeInsert;
use truthdb_core::relstore::ctx::OpMode;
use truthdb_core::storage::StorageError;

use crate::{
    ObjFile, ObjTxn, TABLE_BUCKETS_TREE, TABLE_IDS_TREE, TABLE_NAMESPACES_TREE,
    TABLE_POLICIES_TREE, TABLES_TREE,
};

/// Why a catalog change was refused (the storage itself worked).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableError {
    NoSuchTableBucket,
    NoSuchNamespace,
    NoSuchTable,
    /// The name is taken.
    Exists,
    /// A table bucket still has namespaces, or a namespace tables.
    NotEmpty,
    /// The version token given is not the table's.
    VersionMismatch,
}

/// A table bucket as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBucketInfo {
    pub name: String,
    pub id: String,
    pub created_ms: u64,
}

/// A namespace as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub name: String,
    pub id: String,
    pub created_ms: u64,
    pub created_by: String,
}

/// A table as listed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub namespace: String,
    pub name: String,
    pub id: String,
    pub created_ms: u64,
    pub created_by: String,
    pub modified_ms: u64,
    pub modified_by: String,
    /// Its current metadata file: none until one is set.
    pub metadata_location: Option<String>,
    pub version_token: String,
    pub warehouse_location: String,
}

fn corrupt(what: &str) -> StorageError {
    StorageError::InvalidFile(format!("corrupt {what} row"))
}

/// The key of `parts`, joined by 0 bytes (no name has one).
fn key(parts: &[&str]) -> Vec<u8> {
    parts.join("\0").into_bytes()
}

/// The key prefix of a table bucket's policy (`table` none) or a table's
/// (by its id): a marker after the bucket keeps a bucket policy's chunks
/// apart from its tables' policies.
fn policy_prefix(account: &str, bucket: &str, table: Option<&str>) -> Vec<u8> {
    let mut prefix = key(&[account, bucket]);
    match table {
        None => prefix.extend_from_slice(&[0, 1]),
        Some(id) => {
            prefix.extend_from_slice(&[0, 2]);
            prefix.extend_from_slice(id.as_bytes());
            prefix.push(0);
        }
    }
    prefix
}

/// The keys after `parts` (and a 0 byte), up to the first past them.
fn range(parts: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let mut lower = key(parts);
    lower.push(0);
    let mut upper = key(parts);
    upper.push(1);
    (lower, upper)
}

fn text(row: &Value, field: &str, what: &str) -> Result<String, StorageError> {
    row[field]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| corrupt(what))
}

fn number(row: &Value, field: &str, what: &str) -> Result<u64, StorageError> {
    row[field].as_u64().ok_or_else(|| corrupt(what))
}

fn parse(row: &[u8], what: &str) -> Result<Value, StorageError> {
    serde_json::from_slice(row).map_err(|_| corrupt(what))
}

fn bucket_info(name: &str, row: &[u8]) -> Result<TableBucketInfo, StorageError> {
    let row = parse(row, "table bucket")?;
    Ok(TableBucketInfo {
        name: name.to_string(),
        id: text(&row, "id", "table bucket")?,
        created_ms: number(&row, "created_ms", "table bucket")?,
    })
}

fn namespace_info(name: &str, row: &[u8]) -> Result<NamespaceInfo, StorageError> {
    let row = parse(row, "namespace")?;
    Ok(NamespaceInfo {
        name: name.to_string(),
        id: text(&row, "id", "namespace")?,
        created_ms: number(&row, "created_ms", "namespace")?,
        created_by: text(&row, "created_by", "namespace")?,
    })
}

fn table_info(namespace: &str, name: &str, row: &[u8]) -> Result<TableInfo, StorageError> {
    let row = parse(row, "table")?;
    Ok(TableInfo {
        namespace: namespace.to_string(),
        name: name.to_string(),
        id: text(&row, "id", "table")?,
        created_ms: number(&row, "created_ms", "table")?,
        created_by: text(&row, "created_by", "table")?,
        modified_ms: number(&row, "modified_ms", "table")?,
        modified_by: text(&row, "modified_by", "table")?,
        metadata_location: row["metadata_location"].as_str().map(str::to_string),
        version_token: text(&row, "version_token", "table")?,
        warehouse_location: text(&row, "warehouse_location", "table")?,
    })
}

fn table_row(table: &TableInfo) -> Vec<u8> {
    json!({
        "id": table.id,
        "created_ms": table.created_ms,
        "created_by": table.created_by,
        "modified_ms": table.modified_ms,
        "modified_by": table.modified_by,
        "metadata_location": table.metadata_location,
        "version_token": table.version_token,
        "warehouse_location": table.warehouse_location,
    })
    .to_string()
    .into_bytes()
}

/// Where a list's scan starts: past `after` (the smallest key above it),
/// or at `prefix`, within the range of `parts`.
fn start(parts: &[&str], prefix: &str, after: Option<&str>) -> Vec<u8> {
    let mut at_prefix = parts.to_vec();
    at_prefix.push(prefix);
    let from = key(&at_prefix);
    match after {
        Some(after) => {
            let mut past = parts.to_vec();
            past.push(after);
            let mut past = key(&past);
            past.push(0);
            past.max(from)
        }
        None => from,
    }
}

/// The last part of a key (after its last 0 byte).
fn last(key: &[u8]) -> Result<String, StorageError> {
    let at = key
        .iter()
        .rposition(|byte| *byte == 0)
        .map_or(0, |at| at + 1);
    String::from_utf8(key[at..].to_vec()).map_err(|_| corrupt("table catalog key"))
}

/// The table bucket keys of `account`.
pub(crate) fn account_range(account: &str) -> (Vec<u8>, Vec<u8>) {
    range(&[account])
}

impl ObjTxn<'_, '_> {
    /// Makes table bucket `name` of `account`, with id `id`.
    pub fn create_table_bucket(
        &mut self,
        account: &str,
        name: &str,
        id: &str,
        now_ms: u64,
    ) -> Result<Result<(), TableError>, StorageError> {
        let row = json!({ "id": id, "created_ms": now_ms }).to_string();
        Ok(
            match self.trees.tree(TABLE_BUCKETS_TREE).insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &key(&[account, name]),
                row.as_bytes(),
            )? {
                TreeInsert::Inserted => Ok(()),
                TreeInsert::DuplicateKey => Err(TableError::Exists),
            },
        )
    }

    /// Deletes table bucket `name` of `account`, which must have no
    /// namespace.
    pub fn delete_table_bucket(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Result<(), TableError>, StorageError> {
        let buckets = self.trees.tree(TABLE_BUCKETS_TREE);
        let row_key = key(&[account, name]);
        if buckets.get(self.ctx, &row_key)?.is_none() {
            return Ok(Err(TableError::NoSuchTableBucket));
        }
        let (lower, upper) = range(&[account, name]);
        if !self
            .trees
            .tree(TABLE_NAMESPACES_TREE)
            .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
            .is_empty()
        {
            return Ok(Err(TableError::NotEmpty));
        }
        buckets.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
        let policies = self.trees.tree(TABLE_POLICIES_TREE);
        self.delete_document(&policies, &policy_prefix(account, name, None))?;
        Ok(Ok(()))
    }

    /// Sets the policy of `account`'s table bucket `bucket`, or (`table`,
    /// by id) of one of its tables.
    pub fn put_table_policy(
        &mut self,
        (account, bucket): (&str, &str),
        table: Option<&str>,
        policy: &[u8],
    ) -> Result<Result<(), TableError>, StorageError> {
        if let Err(err) = self.policy_holder((account, bucket), table)? {
            return Ok(Err(err));
        }
        let policies = self.trees.tree(TABLE_POLICIES_TREE);
        self.put_document(policies, &policy_prefix(account, bucket, table), policy)?;
        Ok(Ok(()))
    }

    /// Deletes the policy [`ObjTxn::put_table_policy`] sets (none is no
    /// error).
    pub fn delete_table_policy(
        &mut self,
        (account, bucket): (&str, &str),
        table: Option<&str>,
    ) -> Result<Result<(), TableError>, StorageError> {
        if let Err(err) = self.policy_holder((account, bucket), table)? {
            return Ok(Err(err));
        }
        let policies = self.trees.tree(TABLE_POLICIES_TREE);
        self.delete_document(&policies, &policy_prefix(account, bucket, table))?;
        Ok(Ok(()))
    }

    /// Whether the table bucket (and the table of id `table`) a policy is
    /// of exists.
    fn policy_holder(
        &mut self,
        (account, bucket): (&str, &str),
        table: Option<&str>,
    ) -> Result<Result<(), TableError>, StorageError> {
        if self
            .trees
            .tree(TABLE_BUCKETS_TREE)
            .get(self.ctx, &key(&[account, bucket]))?
            .is_none()
        {
            return Ok(Err(TableError::NoSuchTableBucket));
        }
        if let Some(id) = table
            && self
                .trees
                .tree(TABLE_IDS_TREE)
                .get(self.ctx, &key(&[account, bucket, id]))?
                .is_none()
        {
            return Ok(Err(TableError::NoSuchTable));
        }
        Ok(Ok(()))
    }

    /// Makes namespace `namespace` of `account`'s table bucket `bucket`.
    pub fn create_namespace(
        &mut self,
        (account, bucket): (&str, &str),
        namespace: &str,
        id: &str,
        (created_by, now_ms): (&str, u64),
    ) -> Result<Result<(), TableError>, StorageError> {
        if self
            .trees
            .tree(TABLE_BUCKETS_TREE)
            .get(self.ctx, &key(&[account, bucket]))?
            .is_none()
        {
            return Ok(Err(TableError::NoSuchTableBucket));
        }
        let row = json!({ "id": id, "created_ms": now_ms, "created_by": created_by }).to_string();
        Ok(
            match self.trees.tree(TABLE_NAMESPACES_TREE).insert_unique(
                self.ctx,
                &mut OpMode::Txn(self.txn),
                &key(&[account, bucket, namespace]),
                row.as_bytes(),
            )? {
                TreeInsert::Inserted => Ok(()),
                TreeInsert::DuplicateKey => Err(TableError::Exists),
            },
        )
    }

    /// Deletes a namespace, which must have no table.
    pub fn delete_namespace(
        &mut self,
        (account, bucket): (&str, &str),
        namespace: &str,
    ) -> Result<Result<(), TableError>, StorageError> {
        let namespaces = self.trees.tree(TABLE_NAMESPACES_TREE);
        let row_key = key(&[account, bucket, namespace]);
        if namespaces.get(self.ctx, &row_key)?.is_none() {
            return Ok(Err(TableError::NoSuchNamespace));
        }
        let (lower, upper) = range(&[account, bucket, namespace]);
        if !self
            .trees
            .tree(TABLES_TREE)
            .scan_range_limit(self.ctx, Some(&lower), Some(&upper), 1)?
            .is_empty()
        {
            return Ok(Err(TableError::NotEmpty));
        }
        namespaces.delete(self.ctx, &mut OpMode::Txn(self.txn), &row_key)?;
        Ok(Ok(()))
    }

    /// Makes table `table` (its namespace, name, id and the rest) in
    /// `account`'s table bucket `bucket`.
    pub fn create_table(
        &mut self,
        (account, bucket): (&str, &str),
        table: &TableInfo,
    ) -> Result<Result<(), TableError>, StorageError> {
        if self
            .trees
            .tree(TABLE_BUCKETS_TREE)
            .get(self.ctx, &key(&[account, bucket]))?
            .is_none()
        {
            return Ok(Err(TableError::NoSuchTableBucket));
        }
        if self
            .trees
            .tree(TABLE_NAMESPACES_TREE)
            .get(self.ctx, &key(&[account, bucket, &table.namespace]))?
            .is_none()
        {
            return Ok(Err(TableError::NoSuchNamespace));
        }
        let inserted = self.trees.tree(TABLES_TREE).insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, &table.namespace, &table.name]),
            &table_row(table),
        )?;
        if inserted == TreeInsert::DuplicateKey {
            return Ok(Err(TableError::Exists));
        }
        self.trees.tree(TABLE_IDS_TREE).insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, &table.id]),
            &key(&[&table.namespace, &table.name]),
        )?;
        Ok(Ok(()))
    }

    /// The table, read in this transaction.
    fn table_in(
        &mut self,
        (account, bucket): (&str, &str),
        (namespace, name): (&str, &str),
    ) -> Result<Option<TableInfo>, StorageError> {
        self.trees
            .tree(TABLES_TREE)
            .get(self.ctx, &key(&[account, bucket, namespace, name]))?
            .map(|row| table_info(namespace, name, &row))
            .transpose()
    }

    /// Deletes the table of id `id` at that name, if `version_token` (when
    /// given) is its own: one of the name made since is not it.
    pub fn delete_table(
        &mut self,
        (account, bucket): (&str, &str),
        (namespace, name, id): (&str, &str, &str),
        version_token: Option<&str>,
    ) -> Result<Result<TableInfo, TableError>, StorageError> {
        let Some(table) = self
            .table_in((account, bucket), (namespace, name))?
            .filter(|table| table.id == id)
        else {
            return Ok(Err(TableError::NoSuchTable));
        };
        if version_token.is_some_and(|token| token != table.version_token) {
            return Ok(Err(TableError::VersionMismatch));
        }
        self.trees.tree(TABLES_TREE).delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, namespace, name]),
        )?;
        self.trees.tree(TABLE_IDS_TREE).delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, &table.id]),
        )?;
        let policies = self.trees.tree(TABLE_POLICIES_TREE);
        self.delete_document(&policies, &policy_prefix(account, bucket, Some(&table.id)))?;
        Ok(Ok(table))
    }

    /// Renames the table of id `id` (to another namespace of its bucket, or
    /// another name), if `version_token` (when given) is its own: its id
    /// stays, and one of the name made since is not it.
    #[allow(clippy::too_many_arguments)]
    pub fn rename_table(
        &mut self,
        (account, bucket): (&str, &str),
        (namespace, name, id): (&str, &str, &str),
        (to_namespace, to_name): (&str, &str),
        version_token: Option<&str>,
        (modified_by, now_ms): (&str, u64),
    ) -> Result<Result<(), TableError>, StorageError> {
        let Some(mut table) = self
            .table_in((account, bucket), (namespace, name))?
            .filter(|table| table.id == id)
        else {
            return Ok(Err(TableError::NoSuchTable));
        };
        if version_token.is_some_and(|token| token != table.version_token) {
            return Ok(Err(TableError::VersionMismatch));
        }
        if (namespace, name) == (to_namespace, to_name) {
            return Ok(Ok(()));
        }
        if self
            .trees
            .tree(TABLE_NAMESPACES_TREE)
            .get(self.ctx, &key(&[account, bucket, to_namespace]))?
            .is_none()
        {
            return Ok(Err(TableError::NoSuchNamespace));
        }
        if self
            .table_in((account, bucket), (to_namespace, to_name))?
            .is_some()
        {
            return Ok(Err(TableError::Exists));
        }
        let tables = self.trees.tree(TABLES_TREE);
        tables.delete(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, namespace, name]),
        )?;
        table.modified_by = modified_by.to_string();
        table.modified_ms = now_ms;
        tables.insert_unique(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, to_namespace, to_name]),
            &table_row(&table),
        )?;
        self.trees.tree(TABLE_IDS_TREE).update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, &table.id]),
            &key(&[to_namespace, to_name]),
        )?;
        Ok(Ok(()))
    }

    /// Points the table of id `id` at metadata file `location`, if
    /// `version_token` is its own (compare-and-swap): its token is then
    /// `new_token`. One of the name made since is not it.
    #[allow(clippy::too_many_arguments)]
    pub fn update_metadata_location(
        &mut self,
        (account, bucket): (&str, &str),
        (namespace, name, id): (&str, &str, &str),
        (version_token, new_token): (&str, &str),
        location: &str,
        (modified_by, now_ms): (&str, u64),
    ) -> Result<Result<TableInfo, TableError>, StorageError> {
        let Some(mut table) = self
            .table_in((account, bucket), (namespace, name))?
            .filter(|table| table.id == id)
        else {
            return Ok(Err(TableError::NoSuchTable));
        };
        if version_token != table.version_token {
            return Ok(Err(TableError::VersionMismatch));
        }
        table.metadata_location = Some(location.to_string());
        table.version_token = new_token.to_string();
        table.modified_by = modified_by.to_string();
        table.modified_ms = now_ms;
        self.trees.tree(TABLES_TREE).update(
            self.ctx,
            &mut OpMode::Txn(self.txn),
            &key(&[account, bucket, namespace, name]),
            &table_row(&table),
        )?;
        Ok(Ok(table))
    }
}

impl ObjFile<'_> {
    /// The policy of `account`'s table bucket `bucket`, or (`table`, by
    /// id) of one of its tables.
    pub fn table_policy(
        &mut self,
        (account, bucket): (&str, &str),
        table: Option<&str>,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(tree) = self.existing_tree(TABLE_POLICIES_TREE)? else {
            return Ok(None);
        };
        crate::documents::read(
            &mut self.core.page_ctx(0),
            &tree,
            &policy_prefix(account, bucket, table),
        )
    }

    pub fn table_bucket(
        &mut self,
        account: &str,
        name: &str,
    ) -> Result<Option<TableBucketInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLE_BUCKETS_TREE)? else {
            return Ok(None);
        };
        tree.get(&mut self.core.page_ctx(0), &key(&[account, name]))?
            .map(|row| bucket_info(name, &row))
            .transpose()
    }

    /// `account`'s table buckets whose names start with `prefix`, after
    /// `after`, at most `limit`.
    pub fn list_table_buckets(
        &mut self,
        account: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TableBucketInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLE_BUCKETS_TREE)? else {
            return Ok(Vec::new());
        };
        let (_, upper) = range(&[account]);
        let lower = start(&[account], prefix, after);
        let mut out = Vec::new();
        for (row_key, row) in tree.scan_range_limit(
            &mut self.core.page_ctx(0),
            Some(&lower),
            Some(&upper),
            limit,
        )? {
            let name = last(&row_key)?;
            if !name.starts_with(prefix) {
                break;
            }
            out.push(bucket_info(&name, &row)?);
        }
        Ok(out)
    }

    pub fn namespace(
        &mut self,
        (account, bucket): (&str, &str),
        namespace: &str,
    ) -> Result<Option<NamespaceInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLE_NAMESPACES_TREE)? else {
            return Ok(None);
        };
        tree.get(
            &mut self.core.page_ctx(0),
            &key(&[account, bucket, namespace]),
        )?
        .map(|row| namespace_info(namespace, &row))
        .transpose()
    }

    pub fn list_namespaces(
        &mut self,
        (account, bucket): (&str, &str),
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<NamespaceInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLE_NAMESPACES_TREE)? else {
            return Ok(Vec::new());
        };
        let (_, upper) = range(&[account, bucket]);
        let lower = start(&[account, bucket], prefix, after);
        let mut out = Vec::new();
        for (row_key, row) in tree.scan_range_limit(
            &mut self.core.page_ctx(0),
            Some(&lower),
            Some(&upper),
            limit,
        )? {
            let name = last(&row_key)?;
            if !name.starts_with(prefix) {
                break;
            }
            out.push(namespace_info(&name, &row)?);
        }
        Ok(out)
    }

    pub fn table(
        &mut self,
        (account, bucket): (&str, &str),
        (namespace, name): (&str, &str),
    ) -> Result<Option<TableInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLES_TREE)? else {
            return Ok(None);
        };
        tree.get(
            &mut self.core.page_ctx(0),
            &key(&[account, bucket, namespace, name]),
        )?
        .map(|row| table_info(namespace, name, &row))
        .transpose()
    }

    /// The table of id `id` in `account`'s table bucket `bucket`.
    pub fn table_by_id(
        &mut self,
        (account, bucket): (&str, &str),
        id: &str,
    ) -> Result<Option<TableInfo>, StorageError> {
        let Some(ids) = self.existing_tree(TABLE_IDS_TREE)? else {
            return Ok(None);
        };
        let Some(named) = ids.get(&mut self.core.page_ctx(0), &key(&[account, bucket, id]))? else {
            return Ok(None);
        };
        let named = String::from_utf8(named).map_err(|_| corrupt("table id"))?;
        let (namespace, name) = named.split_once('\0').ok_or_else(|| corrupt("table id"))?;
        self.table((account, bucket), (namespace, name))
    }

    /// The tables of `account`'s table bucket `bucket` (of `namespace`,
    /// when given) whose names start with `prefix`, after the table
    /// `after` (its namespace and name), at most `limit`, by namespace and
    /// name.
    pub fn list_tables(
        &mut self,
        (account, bucket): (&str, &str),
        namespace: Option<&str>,
        prefix: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<Vec<TableInfo>, StorageError> {
        let Some(tree) = self.existing_tree(TABLES_TREE)? else {
            return Ok(Vec::new());
        };
        let (lower, upper) = match namespace {
            Some(namespace) => range(&[account, bucket, namespace]),
            None => range(&[account, bucket]),
        };
        // Past `after` when given (the smallest key above it).
        let lower = match after {
            Some((space, name)) => {
                let mut past = key(&[account, bucket, space, name]);
                past.push(0);
                past.max(lower)
            }
            None => lower,
        };
        let mut out = Vec::new();
        let mut from = lower;
        // A page of the rows (prefix is per namespace: filtered here).
        while out.len() < limit {
            let rows = tree.scan_range_limit(
                &mut self.core.page_ctx(0),
                Some(&from),
                Some(&upper),
                limit,
            )?;
            let Some((last_key, _)) = rows.last() else {
                break;
            };
            let mut next = last_key.clone();
            next.push(0);
            let full = rows.len() == limit;
            for (row_key, row) in rows {
                let text = String::from_utf8(row_key).map_err(|_| corrupt("table key"))?;
                let mut parts = text.rsplitn(3, '\0');
                let (Some(name), Some(space)) = (parts.next(), parts.next()) else {
                    return Err(corrupt("table key"));
                };
                if name.starts_with(prefix) {
                    out.push(table_info(space, name, &row)?);
                    if out.len() == limit {
                        break;
                    }
                }
            }
            if !full {
                break;
            }
            from = next;
        }
        Ok(out)
    }

    /// Whether `account` has any table bucket.
    pub fn has_table_buckets(&mut self, account: &str) -> Result<bool, StorageError> {
        Ok(!self.list_table_buckets(account, "", None, 1)?.is_empty())
    }
}
