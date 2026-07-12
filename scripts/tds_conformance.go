// TDS conformance smoke: drive the TruthDB TDS gateway with go-mssqldb, an
// independent TDS client — create/insert/select/error/login-fail. This is the
// second driver (alongside scripts/tds_conformance.py) so a shared spec
// misunderstanding in either client is unlikely to pass both.
//
// Usage: go run . <host> <port> <user> <password>
// Exits non-zero on any mismatch. Uses its own table so it can run against the
// same live server as the Python conformance without colliding.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

func dsn(host, port, user, pass string) string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=truthdb&encrypt=disable",
		user, pass, host, port)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 5 {
		fail("usage: tds_conformance <host> <port> <user> <password>")
	}
	host, port, user, pass := os.Args[1], os.Args[2], os.Args[3], os.Args[4]

	// 1. Login failure must be reported (not a hang / crash).
	badDB, err := sql.Open("sqlserver", dsn(host, port, user, "definitely-wrong"))
	if err != nil {
		fail("open (bad password): %v", err)
	}
	if err := badDB.Ping(); err == nil {
		fail("bad password unexpectedly connected")
	}
	badDB.Close()

	db, err := sql.Open("sqlserver", dsn(host, port, user, pass))
	if err != nil {
		fail("open: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fail("connect: %v", err)
	}

	mustExec(db, "CREATE TABLE products_go (id INT NOT NULL PRIMARY KEY, "+
		"name NVARCHAR(50), price FLOAT, active BIT)")
	mustExec(db, "INSERT INTO products_go VALUES (1, 'Skor', 79.5, 1), "+
		"(2, 'Kangor', 129.0, 0), (3, NULL, NULL, NULL)")

	// 2. Typed SELECT: values round-trip with their SQL types, NULLs included.
	rows, err := db.Query("SELECT id, name, price, active FROM products_go ORDER BY id")
	if err != nil {
		fail("SELECT: %v", err)
	}
	type record struct {
		id     int64
		name   sql.NullString
		price  sql.NullFloat64
		active sql.NullBool
	}
	var got []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.id, &r.name, &r.price, &r.active); err != nil {
			fail("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		fail("rows: %v", err)
	}
	want := []record{
		{1, sql.NullString{String: "Skor", Valid: true}, sql.NullFloat64{Float64: 79.5, Valid: true}, sql.NullBool{Bool: true, Valid: true}},
		{2, sql.NullString{String: "Kangor", Valid: true}, sql.NullFloat64{Float64: 129.0, Valid: true}, sql.NullBool{Bool: false, Valid: true}},
		{3, sql.NullString{}, sql.NullFloat64{}, sql.NullBool{}},
	}
	if fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		fail("SELECT mismatch\n got: %v\n want: %v", got, want)
	}

	// 3. Computed column comes back typed (int, not text).
	var id, doubled int64
	if err := db.QueryRow("SELECT id, id * 2 AS doubled FROM products_go WHERE id = 2").
		Scan(&id, &doubled); err != nil {
		fail("computed column: %v", err)
	}
	if id != 2 || doubled != 4 {
		fail("computed column mismatch: id=%d doubled=%d", id, doubled)
	}

	// 4. Error path: duplicate PK must surface SQL Server error 2627.
	_, err = db.Exec("INSERT INTO products_go VALUES (1, 'dup', 0, 1)")
	if err == nil {
		fail("duplicate PK did not raise")
	}
	var mssqlErr mssql.Error
	if !errors.As(err, &mssqlErr) || mssqlErr.Number != 2627 {
		fail("expected error 2627, got: %v", err)
	}

	// 5. sys.tables is queryable.
	var name string
	if err := db.QueryRow("SELECT name FROM sys.tables WHERE name = 'products_go'").
		Scan(&name); err != nil {
		fail("sys.tables missing products_go: %v", err)
	}

	// 6. Bare-column alias is carried in the result metadata (COLMETADATA).
	aliasRows, err := db.Query("SELECT id AS thing FROM products_go WHERE id = 1")
	if err != nil {
		fail("alias query: %v", err)
	}
	cols, err := aliasRows.Columns()
	aliasRows.Close()
	if err != nil || len(cols) != 1 || cols[0] != "thing" {
		fail("alias not surfaced: %v (%v)", cols, err)
	}

	// 7. VARCHAR (BIGVARCHR / CP1252) round-trips non-ASCII text and NULL.
	mustExec(db, "CREATE TABLE vtext_go (id INT NOT NULL PRIMARY KEY, s VARCHAR(20))")
	mustExec(db, "INSERT INTO vtext_go VALUES (1, 'café'), (2, 'Zürich'), (3, NULL)")
	vwant := []sql.NullString{
		{String: "café", Valid: true},
		{String: "Zürich", Valid: true},
		{},
	}
	if got := scanNullStrings(db, "SELECT s FROM vtext_go ORDER BY id"); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", vwant) {
		fail("VARCHAR round-trip: got %v want %v", got, vwant)
	}

	// 8. NULL in a nullable INT column round-trips (INTN zero-length form).
	mustExec(db, "CREATE TABLE nints_go (id INT NOT NULL PRIMARY KEY, n INT)")
	mustExec(db, "INSERT INTO nints_go VALUES (1, 42), (2, NULL)")
	nwant := []sql.NullInt64{{Int64: 42, Valid: true}, {}}
	if got := scanNullInts(db, "SELECT n FROM nints_go ORDER BY id"); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", nwant) {
		fail("NULL INT round-trip: got %v want %v", got, nwant)
	}

	// 9. Multi-statement batch yields multiple result sets (DONE_MORE path).
	// database/sql only surfaces the next set once the current one is drained.
	mrows, err := db.Query("SELECT 1 AS a; SELECT 2 AS b")
	if err != nil {
		fail("multi-result query: %v", err)
	}
	if first := drainInts(mrows); len(first) != 1 || first[0] != 1 {
		fail("first result set wrong: %v", first)
	}
	if !mrows.NextResultSet() {
		fail("expected a second result set: %v", mrows.Err())
	}
	if second := drainInts(mrows); len(second) != 1 || second[0] != 2 {
		fail("second result set wrong: %v", second)
	}
	mrows.Close()

	// 10. A result larger than one 4096-byte packet reassembles intact. A
	// computed literal (6000 bytes UCS-2) avoids the heap in-row size cap.
	big := strings.Repeat("x", 3000)
	var large string
	if err := db.QueryRow("SELECT '" + big + "' AS big").Scan(&large); err != nil {
		fail("large select: %v", err)
	}
	if large != big {
		fail("large multi-packet value: len %d != 3000", len(large))
	}

	// 11-13: transactions via the TDS Transaction Manager (db.BeginTx sends
	// TM_BEGIN_XACT, tx.Commit/Rollback send TM_COMMIT/ROLLBACK_XACT).
	transactionMatrix(db)
	blockingDemo(db, host, port, user, pass)

	fmt.Println("tds conformance (go-mssqldb): OK")
}

// transactionMatrix exercises db.BeginTx + Commit/Rollback (the TM request
// path) and verifies commit durability and rollback discard.
func transactionMatrix(db *sql.DB) {
	ctx := context.Background()
	mustExec(db, "CREATE TABLE tx_go (id INT NOT NULL PRIMARY KEY, v INT)")

	// 11. BeginTx + Insert + Commit → the row persists.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		fail("BeginTx (commit case): %v", err)
	}
	if _, err := tx.Exec("INSERT INTO tx_go VALUES (1, 100)"); err != nil {
		fail("tx insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		fail("tx commit: %v", err)
	}
	var v int64
	if err := db.QueryRow("SELECT v FROM tx_go WHERE id = 1").Scan(&v); err != nil {
		fail("committed row missing: %v", err)
	}
	if v != 100 {
		fail("committed value wrong: %d", v)
	}

	// 12. BeginTx + Insert + Rollback → the row is discarded.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		fail("BeginTx (rollback case): %v", err)
	}
	if _, err := tx2.Exec("INSERT INTO tx_go VALUES (2, 200)"); err != nil {
		fail("tx2 insert: %v", err)
	}
	if err := tx2.Rollback(); err != nil {
		fail("tx2 rollback: %v", err)
	}
	rows := scanNullInts(db, "SELECT id FROM tx_go ORDER BY id")
	if len(rows) != 1 || rows[0].Int64 != 1 {
		fail("rollback did not discard row 2: %v", rows)
	}
}

// blockingDemo shows a two-connection blocking interaction: an uncommitted
// writer blocks a reader on the same table until it commits (READ COMMITTED,
// no dirty read). The reader runs in a goroutine and must stay blocked until
// the writer commits, then observe the committed value.
func blockingDemo(db *sql.DB, host, port, user, pass string) {
	mustExec(db, "CREATE TABLE block_go (id INT NOT NULL PRIMARY KEY, v INT)")
	mustExec(db, "INSERT INTO block_go VALUES (1, 1)")

	// A second, independent connection for the writer.
	writerDB, err := sql.Open("sqlserver", dsn(host, port, user, pass))
	if err != nil {
		fail("open writer db: %v", err)
	}
	defer writerDB.Close()

	writerTx, err := writerDB.BeginTx(context.Background(), nil)
	if err != nil {
		fail("writer BeginTx: %v", err)
	}
	if _, err := writerTx.Exec("UPDATE block_go SET v = 2 WHERE id = 1"); err != nil {
		fail("writer update: %v", err)
	}

	// The reader (independent connection) blocks on the writer's X lock.
	value := make(chan int64, 1)
	readErr := make(chan error, 1)
	go func() {
		var v int64
		if err := db.QueryRow("SELECT v FROM block_go WHERE id = 1").Scan(&v); err != nil {
			readErr <- err
			return
		}
		value <- v
	}()

	// While the writer's transaction is open, the reader must not return.
	select {
	case <-value:
		fail("reader was not blocked by the uncommitted writer (dirty read?)")
	case err := <-readErr:
		fail("reader errored while blocked: %v", err)
	case <-time.After(400 * time.Millisecond):
		// Still blocked, as expected.
	}

	// Committing the writer releases the lock; the reader unblocks and sees
	// the committed value (2), never the original (1).
	if err := writerTx.Commit(); err != nil {
		fail("writer commit: %v", err)
	}
	select {
	case v := <-value:
		if v != 2 {
			fail("reader saw %d, expected the committed value 2", v)
		}
	case err := <-readErr:
		fail("reader errored after commit: %v", err)
	case <-time.After(5 * time.Second):
		fail("reader did not unblock after the writer committed")
	}
}

func scanNullStrings(db *sql.DB, query string) []sql.NullString {
	rows, err := db.Query(query)
	if err != nil {
		fail("query %q: %v", query, err)
	}
	defer rows.Close()
	var out []sql.NullString
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			fail("scan: %v", err)
		}
		out = append(out, v)
	}
	return out
}

// drainInts reads every row of the current result set as int64 (advancing the
// cursor so a following NextResultSet can surface the next set).
func drainInts(rows *sql.Rows) []int64 {
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			fail("scan int: %v", err)
		}
		out = append(out, v)
	}
	return out
}

func scanNullInts(db *sql.DB, query string) []sql.NullInt64 {
	rows, err := db.Query(query)
	if err != nil {
		fail("query %q: %v", query, err)
	}
	defer rows.Close()
	var out []sql.NullInt64
	for rows.Next() {
		var v sql.NullInt64
		if err := rows.Scan(&v); err != nil {
			fail("scan: %v", err)
		}
		out = append(out, v)
	}
	return out
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		fail("exec %q: %v", query, err)
	}
}
