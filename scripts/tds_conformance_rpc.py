#!/usr/bin/env python3
"""TDS RPC conformance smoke: parameterized queries over sp_executesql.

pytds sends `cur.execute(sql, params)` as an sp_executesql RPC (packet type
0x03), so this exercises the RPC parameter decoder end to end — separately from
the plain-batch path in tds_conformance.py.

Usage: tds_conformance_rpc.py <host> <port> <user> <password>
Exits non-zero on any mismatch.
"""
import datetime
import decimal
import sys

import pytds


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    conn = pytds.connect(
        host, "truthdb", user, password, port=port, login_timeout=10, autocommit=True
    )
    cur = conn.cursor()

    cur.execute(
        "CREATE TABLE rpc_conf (id INT NOT NULL PRIMARY KEY, name NVARCHAR(40), "
        "amount DECIMAL(10,2), n INT)"
    )

    # Parameterized INSERTs: int, unicode string, decimal, and a NULL parameter.
    cur.execute(
        "INSERT INTO rpc_conf (id, name, amount, n) VALUES (%s, %s, %s, %s)",
        (1, "café", decimal.Decimal("12.34"), 7),
    )
    cur.execute(
        "INSERT INTO rpc_conf (id, name, amount, n) VALUES (%s, %s, %s, %s)",
        (2, "Zürich", decimal.Decimal("-5.00"), None),
    )

    # Parameterized SELECT: the predicate value arrives as a typed parameter.
    cur.execute("SELECT id, name, amount, n FROM rpc_conf WHERE id = %s", (1,))
    row = cur.fetchone()
    want = (1, "café", decimal.Decimal("12.34"), 7)
    if row != want:
        print(f"FAIL: param SELECT mismatch\n got: {row}\n want: {want}")
        return 1

    # NULL parameter round-trips as a NULL column.
    cur.execute("SELECT n FROM rpc_conf WHERE id = %s", (2,))
    if cur.fetchone()[0] is not None:
        print("FAIL: NULL parameter did not round-trip")
        return 1

    # Injection safety: a payload passed as a *parameter* is stored literally,
    # never executed. If parameters were spliced into SQL text this would drop
    # the table.
    evil = "'); DROP TABLE rpc_conf; --"
    cur.execute("INSERT INTO rpc_conf (id, name) VALUES (%s, %s)", (3, evil))
    cur.execute("SELECT name FROM rpc_conf WHERE id = %s", (3,))
    if cur.fetchone()[0] != evil:
        print("FAIL: parameter was not stored literally")
        return 1
    cur.execute("SELECT COUNT(*) FROM rpc_conf")
    if cur.fetchone()[0] != 3:
        print("FAIL: table did not survive the injection payload")
        return 1

    # A long string parameter (> 4000 chars) is sent NVARCHAR(MAX)/PLP-chunked.
    # Check its decoded length (a >4000-char NVARCHAR result value is a separate,
    # not-yet-supported MAX case, so measure rather than echo it).
    long = "λ" * 5000
    cur.execute("SELECT LEN(%s)", (long,))
    if cur.fetchone()[0] != 5000:
        print("FAIL: long (PLP) parameter was not fully decoded")
        return 1

    # A DATE/DATETIME2 parameter round-trips through the temporal decoders.
    cur.execute("CREATE TABLE rpc_time (id INT NOT NULL PRIMARY KEY, d DATE, ts DATETIME2)")
    cur.execute(
        "INSERT INTO rpc_time (id, d, ts) VALUES (%s, %s, %s)",
        (1, datetime.date(2021, 3, 9), datetime.datetime(2021, 3, 9, 8, 30, 15, 250000)),
    )
    cur.execute("SELECT d, ts FROM rpc_time WHERE id = %s", (1,))
    d, ts = cur.fetchone()
    if d != datetime.date(2021, 3, 9):
        print(f"FAIL: DATE parameter round-trip: {d!r}")
        return 1
    if ts != datetime.datetime(2021, 3, 9, 8, 30, 15, 250000):
        print(f"FAIL: DATETIME2 parameter round-trip: {ts!r}")
        return 1

    conn.close()
    print("tds rpc conformance: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
