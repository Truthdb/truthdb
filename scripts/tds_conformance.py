#!/usr/bin/env python3
"""TDS conformance smoke: drive the TruthDB TDS gateway with an independent
TDS client (python-tds / pytds) — create/insert/select/error/login-fail.

Usage: tds_conformance.py <host> <port> <user> <password>
Exits non-zero on any mismatch.
"""
import sys
import pytds


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

    # 1. Login failure must be reported (not a hang / crash).
    try:
        pytds.connect(host, "truthdb", user, "definitely-wrong", port=port, login_timeout=10)
        print("FAIL: bad password unexpectedly connected")
        return 1
    except pytds.Error:
        pass  # expected

    conn = pytds.connect(host, "truthdb", user, password, port=port, login_timeout=10,
                         autocommit=True)
    cur = conn.cursor()

    cur.execute("CREATE TABLE products (id INT NOT NULL PRIMARY KEY, "
                "name NVARCHAR(50), price FLOAT, active BIT)")
    cur.execute("INSERT INTO products VALUES (1, 'Skor', 79.5, 1), "
                "(2, 'Kangor', 129.0, 0), (3, NULL, NULL, NULL)")

    cur.execute("SELECT id, name, price, active FROM products ORDER BY id")
    rows = cur.fetchall()
    expected = [
        (1, "Skor", 79.5, True),
        (2, "Kangor", 129.0, False),
        (3, None, None, None),
    ]
    if rows != expected:
        print(f"FAIL: SELECT mismatch\n got: {rows}\n want: {expected}")
        return 1

    # Typed columns: id should come back as an int, not a string.
    cur.execute("SELECT id, id * 2 AS doubled FROM products WHERE id = 2")
    row = cur.fetchone()
    if row != (2, 4):
        print(f"FAIL: computed column mismatch: {row}")
        return 1

    # Error path: duplicate PK must raise with the SQL Server number 2627.
    try:
        cur.execute("INSERT INTO products VALUES (1, 'dup', 0, 1)")
        print("FAIL: duplicate PK did not raise")
        return 1
    except pytds.Error as exc:
        if "2627" not in str(exc) and getattr(exc, "number", None) != 2627:
            print(f"FAIL: expected error 2627, got: {exc!r}")
            return 1

    # sys.tables is queryable.
    cur.execute("SELECT name FROM sys.tables")
    names = {r[0] for r in cur.fetchall()}
    if "products" not in names:
        print(f"FAIL: sys.tables missing products: {names}")
        return 1

    # A bare column's alias is carried in the result metadata (COLMETADATA).
    cur.execute("SELECT id AS thing FROM products WHERE id = 1")
    if cur.description[0][0] != "thing":
        print(f"FAIL: alias not surfaced: {cur.description[0][0]!r}")
        return 1

    # VARCHAR (BIGVARCHR / CP1252) round-trips non-ASCII text, and NULL.
    cur.execute("CREATE TABLE vtext (id INT NOT NULL PRIMARY KEY, s VARCHAR(20))")
    cur.execute("INSERT INTO vtext VALUES (1, 'café'), (2, 'Zürich'), (3, NULL)")
    cur.execute("SELECT s FROM vtext ORDER BY id")
    vals = [r[0] for r in cur.fetchall()]
    if vals != ["café", "Zürich", None]:
        print(f"FAIL: VARCHAR round-trip: {vals!r}")
        return 1

    # NULL in a nullable INT column round-trips (INTN zero-length form).
    cur.execute("CREATE TABLE nints (id INT NOT NULL PRIMARY KEY, n INT)")
    cur.execute("INSERT INTO nints VALUES (1, 42), (2, NULL)")
    cur.execute("SELECT n FROM nints ORDER BY id")
    if [r[0] for r in cur.fetchall()] != [42, None]:
        print("FAIL: NULL INT did not round-trip")
        return 1

    # Multi-statement batch yields multiple result sets (DONE_MORE path).
    cur.execute("SELECT 1 AS a; SELECT 2 AS b")
    if cur.fetchall() != [(1,)]:
        print("FAIL: first result set wrong")
        return 1
    if not cur.nextset() or cur.fetchall() != [(2,)]:
        print("FAIL: second result set missing/wrong")
        return 1

    # A result larger than one 4096-byte packet must reassemble intact. A
    # computed literal (6000 bytes UCS-2) avoids the heap in-row size cap.
    big = "x" * 3000
    cur.execute(f"SELECT '{big}' AS big")
    if cur.fetchone()[0] != big:
        print("FAIL: large multi-packet value did not round-trip")
        return 1

    # Stage 5 types over the wire: DECIMAL, DATE, DATETIME2, UNIQUEIDENTIFIER.
    import datetime
    import decimal as decimal_mod
    cur.execute(
        "CREATE TABLE typed (id INT NOT NULL PRIMARY KEY, amount DECIMAL(10,2), "
        "d DATE, ts DATETIME2, g UNIQUEIDENTIFIER)"
    )
    cur.execute(
        "INSERT INTO typed VALUES (1, 1234.56, '2020-06-15', "
        "'2020-06-15 13:45:30.5', '6F9619FF-8B86-D011-B42D-00C04FC964FF')"
    )
    cur.execute("SELECT amount, d, ts, g FROM typed")
    amount, d, ts, g = cur.fetchone()
    if amount != decimal_mod.Decimal("1234.56"):
        print(f"FAIL: DECIMAL round-trip: {amount!r}")
        return 1
    if d != datetime.date(2020, 6, 15):
        print(f"FAIL: DATE round-trip: {d!r}")
        return 1
    if ts != datetime.datetime(2020, 6, 15, 13, 45, 30, 500000):
        print(f"FAIL: DATETIME2 round-trip: {ts!r}")
        return 1
    if str(g).upper() != "6F9619FF-8B86-D011-B42D-00C04FC964FF":
        print(f"FAIL: UNIQUEIDENTIFIER round-trip: {g!r}")
        return 1

    # Stage 6 transactions via the TDS Transaction Manager. DDL runs while
    # autocommit is on (it is disallowed inside an explicit transaction); DML
    # transactions then commit/rollback through TM_COMMIT/ROLLBACK requests.
    cur.execute("CREATE TABLE tx_py (id INT NOT NULL PRIMARY KEY, v INT)")

    conn.autocommit = False
    cur.execute("INSERT INTO tx_py VALUES (1, 100)")
    conn.commit()
    conn.autocommit = True
    cur.execute("SELECT v FROM tx_py WHERE id = 1")
    if cur.fetchone()[0] != 100:
        print("FAIL: committed transaction row missing")
        return 1

    conn.autocommit = False
    cur.execute("INSERT INTO tx_py VALUES (2, 200)")
    conn.rollback()
    conn.autocommit = True
    cur.execute("SELECT id FROM tx_py ORDER BY id")
    if cur.fetchall() != [(1,)]:
        print("FAIL: rolled-back transaction row was not discarded")
        return 1

    # Session-identity intrinsics reflect the connection's database and login.
    cur.execute("SELECT DB_NAME(), SUSER_SNAME(), @@SPID")
    db, login, spid = cur.fetchone()
    if db != "truthdb" or login != "sa" or not (isinstance(spid, int) and spid > 0):
        print(f"FAIL: session intrinsics: db={db!r} login={login!r} spid={spid!r}")
        return 1

    # A statement that fails after its result set began streaming (the server
    # emits rows as the scan runs since #105). The failed set is closed with a
    # clean DONE — an error-flagged DONE without an ERROR token would make
    # this driver raise "Request failed, server didn't send error message"
    # and strand the results behind it.
    cur.execute("CREATE TABLE stream_err (id INT NOT NULL PRIMARY KEY)")
    cur.execute("INSERT INTO stream_err VALUES " +
                ", ".join(f"({i})" for i in range(1, 401)))

    # Caught mid-stream error: the batch succeeds, the CATCH set is readable.
    cur.execute("BEGIN TRY SELECT id FROM stream_err WHERE 10 / (id - 300) > -100 END TRY "
                "BEGIN CATCH SELECT 99 AS caught END CATCH")
    partial = cur.fetchall()
    if len(partial) >= 300:
        print(f"FAIL: caught mid-stream error returned {len(partial)} rows, expected a partial set")
        return 1
    if not cur.nextset():
        print("FAIL: the CATCH result set is unreachable after a caught mid-stream error")
        return 1
    if cur.fetchall() != [(99,)]:
        print("FAIL: the CATCH result set did not arrive intact")
        return 1

    # Caught mid-stream error with an EMPTY catch: the whole batch reads as
    # success (the buffered path sent one clean final DONE for this shape).
    cur.execute("BEGIN TRY SELECT id FROM stream_err WHERE 10 / (id - 300) > -100 END TRY "
                "BEGIN CATCH END CATCH")
    cur.fetchall()
    while cur.nextset():
        cur.fetchall()

    # Continued mid-stream error (XACT_ABORT OFF, in-transaction): the batch
    # runs to the end and the real error text arrives with the final DONE.
    try:
        cur.execute("BEGIN TRANSACTION; "
                    "SELECT id FROM stream_err WHERE 10 / (id - 300) > -100; "
                    "SELECT 7 AS after; COMMIT")
        cur.fetchall()
        while cur.nextset():
            cur.fetchall()
        print("FAIL: the continued divide-by-zero never surfaced")
        return 1
    except pytds.tds_base.Error as e:
        if "divide" not in str(e).lower() and "8134" not in str(e):
            print(f"FAIL: continued error lost its text: {e}")
            return 1

    # Stage 14 SSMS probes over pytds: SERVERPROPERTY, sys.databases, USE,
    # and NOCOUNT (rowcount reads -1 when the DONE carries no count).
    cur.execute("SELECT SERVERPROPERTY('ProductVersion')")
    if cur.fetchone()[0] != "16.0.1000.6":
        print("FAIL: SERVERPROPERTY('ProductVersion')")
        return 1
    cur.execute("SELECT name, database_id FROM sys.databases")
    if cur.fetchone() != ("truthdb", 1):
        print("FAIL: sys.databases row")
        return 1
    cur.execute("USE truthdb")
    cur.execute("SET NOCOUNT ON")
    cur.execute("INSERT INTO nints VALUES (3, 3)")
    if cur.rowcount != -1:
        print(f"FAIL: NOCOUNT insert must report no count, got {cur.rowcount}")
        return 1
    cur.execute("SET NOCOUNT OFF")
    cur.execute("DELETE FROM nints WHERE id = 3")
    if cur.rowcount != 1:
        print(f"FAIL: count must return after NOCOUNT OFF, got {cur.rowcount}")
        return 1

    # Stage 14: NVARCHAR(MAX) — PLP in both directions. pytds sends a long
    # parameter as PLP (the decoder's live pin) and reads the PLP-encoded
    # column back; 100k chars forces the overflow-chain storage path.
    cur.execute("CREATE TABLE maxi (id INT NOT NULL PRIMARY KEY, body NVARCHAR(MAX))")
    payload = "λx." * 33000 + "end"  # 99003 chars, non-ASCII included
    cur.execute("INSERT INTO maxi VALUES (%s, %s)", (1, payload))
    cur.execute("SELECT LEN(body), body FROM maxi WHERE id = 1")
    length, body = cur.fetchone()
    if length != len(payload) or body != payload:
        print(f"FAIL: NVARCHAR(MAX) round-trip: len {length} vs {len(payload)}, "
              f"equal={body == payload}")
        return 1
    cur.execute("SELECT body FROM maxi WHERE body = %s", (payload,))
    if cur.fetchone() is None:
        print("FAIL: NVARCHAR(MAX) equality predicate")
        return 1

    conn.close()
    print("tds conformance: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
