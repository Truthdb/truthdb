use super::*;

#[test]
fn alter_database_set_recovery_parses() {
    let full = Parser::parse_str("ALTER DATABASE CURRENT SET RECOVERY FULL").expect("parse");
    assert!(matches!(
        &full[0],
        Statement::AlterDatabase(a) if a.options == vec![(DatabaseOption::Recovery, true)]
    ));
    let simple = Parser::parse_str("ALTER DATABASE d SET RECOVERY SIMPLE").expect("parse");
    assert!(matches!(
        &simple[0],
        Statement::AlterDatabase(a) if a.options == vec![(DatabaseOption::Recovery, false)]
    ));
    // Only FULL/SIMPLE are supported; any other mode is a syntax error.
    assert!(Parser::parse_str("ALTER DATABASE d SET RECOVERY BULK_LOGGED").is_err());
    // The existing ON/OFF options still parse alongside.
    let rcsi = Parser::parse_str("ALTER DATABASE d SET READ_COMMITTED_SNAPSHOT ON").expect("parse");
    assert!(matches!(
        &rcsi[0],
        Statement::AlterDatabase(a)
            if a.options == vec![(DatabaseOption::ReadCommittedSnapshot, true)]
    ));
}
#[test]
fn backup_database_parses_with_options() {
    let stmts =
        Parser::parse_str("BACKUP DATABASE mydb TO DISK = '/tmp/f.bak' WITH CHECKSUM, COPY_ONLY")
            .expect("parse");
    assert_eq!(stmts.len(), 1);
    match &stmts[0] {
        Statement::BackupDatabase {
            database,
            path,
            checksum,
            copy_only,
            ..
        } => {
            assert_eq!(database.value, "mydb");
            assert_eq!(path, "/tmp/f.bak");
            assert!(*checksum);
            assert!(*copy_only);
        }
        other => panic!("expected BackupDatabase, got {other:?}"),
    }

    // A bare BACKUP defaults CHECKSUM on and COPY_ONLY off.
    let bare = Parser::parse_str("BACKUP DATABASE d TO DISK = 'x'").expect("parse bare");
    assert!(matches!(
        &bare[0],
        Statement::BackupDatabase {
            checksum: true,
            copy_only: false,
            ..
        }
    ));

    // NO_CHECKSUM turns verification off; INIT is accepted (inert).
    let no_ck =
        Parser::parse_str("BACKUP DATABASE d TO DISK = 'x' WITH NO_CHECKSUM, INIT").expect("parse");
    assert!(matches!(
        &no_ck[0],
        Statement::BackupDatabase {
            checksum: false,
            ..
        }
    ));
}
#[test]
fn table_level_primary_key_duplicate_rejected() {
    let sql = "CREATE TABLE t (a INT, PRIMARY KEY (a), PRIMARY KEY (a))";
    assert_eq!(Parser::parse_str(sql).unwrap_err().number, 8110);
    let sql2 = "CREATE TABLE t (id INT PRIMARY KEY, PRIMARY KEY (id))";
    assert_eq!(Parser::parse_str(sql2).unwrap_err().number, 8110);
}
