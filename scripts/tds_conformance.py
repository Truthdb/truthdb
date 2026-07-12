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

    conn.close()
    print("tds conformance: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
