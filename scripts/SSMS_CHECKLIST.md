# SSMS query-window checklist (Stage 14, manual)

TruthDB's SSMS scope is the **query window**: connect, run T-SQL, read
results and messages. Object Explorer works only as far as the catalog
views it queries exist; anything it cannot resolve shows empty, not broken.

Setup: a running TruthDB with TDS enabled (default port 1433), SSMS 19+.

## Connect

- [ ] Connection dialog: server `<host>,1433`, SQL authentication, login
      `sa` + the configured password. Set **Encrypt = Optional** (or
      Mandatory with **Trust server certificate** checked — the certificate
      is self-signed).
- [ ] The connection opens without errors and the status bar shows the
      server name and login.

## New query window

- [ ] `SELECT @@VERSION` returns the TruthDB version banner.
- [ ] `SELECT SERVERPROPERTY('ProductVersion'), SERVERPROPERTY('Edition'),
      SERVERPROPERTY('EngineEdition')` returns `16.0.1000.6`,
      `TruthDB Edition (64-bit)`, `3`.
- [ ] `SELECT name, database_id, state_desc FROM sys.databases` returns one
      row: `truthdb, 1, ONLINE`.
- [ ] `SELECT * FROM sys.configurations` returns the minimal rows without
      error.
- [ ] `USE truthdb` succeeds and the Messages tab shows
      `Changed database context to 'truthdb'.`
- [ ] `USE master` fails with error 911 (single-database instance).

## Results and messages

- [ ] `CREATE TABLE ssms_t (id INT NOT NULL PRIMARY KEY, name NVARCHAR(50));`
      then `INSERT INTO ssms_t VALUES (1, N'ä'), (2, NULL);` shows
      `(2 rows affected)` in Messages.
- [ ] `SELECT * FROM ssms_t` shows the grid with typed columns, the NULL
      cell rendered as NULL, and `(2 rows affected)`.
- [ ] `SET NOCOUNT ON` then another INSERT: no `(n rows affected)` line.
      `SET NOCOUNT OFF` restores it.
- [ ] `SELECT @@ROWCOUNT` directly after the SELECT above returns 2.
- [ ] A duplicate-key INSERT shows `Msg 2627` in Messages with the
      constraint text, and the batch stops.
- [ ] `BEGIN TRAN; UPDATE ssms_t SET name = N'x' WHERE id = 1; ROLLBACK;`
      then SELECT: the original value is back.
- [ ] Two query windows: window A `BEGIN TRAN; UPDATE ssms_t SET name = N'y'
      WHERE id = 1;` (leave open), window B `SELECT * FROM ssms_t` blocks;
      after `ALTER DATABASE truthdb SET READ_COMMITTED_SNAPSHOT ON`
      (fresh windows), the same read returns the pre-update value instantly.
- [ ] Cancel (the red stop button) during a long-running query aborts it
      and the window stays usable.
- [ ] `DROP TABLE ssms_t` cleans up.

## Known limits (expected, not failures)

- Object Explorer's deeper nodes (programmability, security, agent) are
  empty — those catalog views do not exist yet.
- `sp_help`, `sp_who`, IntelliSense metadata RPCs are not implemented;
  IntelliSense may log errors without affecting query execution.
- VARCHAR(MAX)/NVARCHAR(MAX) columns arrive with the overflow-storage work
  (Stage 14, part 2).
