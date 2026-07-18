#!/usr/bin/env python3
"""TDS user-procedure callproc conformance: OUTPUT parameters and RETURN status.

pytds `cur.callproc(name, params)` issues an RPC-by-name (packet 0x03) to a user
stored procedure — distinct from the sp_executesql path in tds_conformance_rpc.py
and the sp_prepare family in tds_conformance_prepared.py. This adjudicates, from a
real driver, the OUTPUT copy-back (RETURNVALUE tokens, one per output ordinal) and
the RETURN status (RETURNSTATUS token) that #128 implemented — including the
per-ordinal placement that a single-OUTPUT test would not catch.

Usage: tds_conformance_procs.py <host> <port> <user> <password>
Exits non-zero on any mismatch.
"""
import sys

import pytds


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    conn = pytds.connect(
        host, "truthdb", user, password, port=port, login_timeout=10, autocommit=True
    )
    cur = conn.cursor()

    # A procedure with one input, two OUTPUT parameters on DISTINCT ordinals, and
    # a RETURN status. The two outputs catch a per-ordinal placement bug (a single
    # output would pass even if every RETURNVALUE were stamped ordinal 0).
    cur.execute(
        "CREATE PROCEDURE add_and_double (@x INT, @sum INT OUTPUT, @doubled INT OUTPUT) AS "
        "BEGIN SET @sum = @x + 10; SET @doubled = @x * 2; RETURN 42 END"
    )

    # Positional callproc: the OUTPUT slots come back filled, in order.
    result = cur.callproc(
        "add_and_double", (5, pytds.output(param_type=int), pytds.output(param_type=int))
    )
    if result[1] != 15 or result[2] != 10:
        print(f"FAIL: positional OUTPUT: got sum={result[1]} doubled={result[2]}, want 15/10")
        return 1
    if cur.return_value != 42:
        print(f"FAIL: RETURN status: got {cur.return_value}, want 42")
        return 1

    # A different input reruns the proc; the outputs track it.
    result = cur.callproc(
        "add_and_double", (100, pytds.output(param_type=int), pytds.output(param_type=int))
    )
    if result[1] != 110 or result[2] != 200:
        print(f"FAIL: second call OUTPUT: got sum={result[1]} doubled={result[2]}, want 110/200")
        return 1

    # Named callproc: parameters supplied by @name, OUTPUT still copied back.
    cur.execute(
        "CREATE PROCEDURE describe (@id INT, @label NVARCHAR(20) OUTPUT) AS "
        "BEGIN SET @label = 'row-' + CAST(@id AS NVARCHAR(10)); RETURN 0 END"
    )
    named = cur.callproc(
        "describe", {"@id": 7, "@label": pytds.output(param_type=str)}
    )
    # The dict callproc returns values positionally in declaration order.
    if named[1] != "row-7":
        print(f"FAIL: named OUTPUT: got label={named[1]!r}, want 'row-7'")
        return 1

    # A negative RETURN status round-trips as a signed int.
    cur.execute(
        "CREATE PROCEDURE classify (@n INT, @kind INT OUTPUT) AS "
        "BEGIN SET @kind = @n; IF @n < 0 RETURN -1; RETURN 1 END"
    )
    cur.callproc("classify", (-5, pytds.output(param_type=int)))
    if cur.return_value != -1:
        print(f"FAIL: negative RETURN status: got {cur.return_value}, want -1")
        return 1
    cur.callproc("classify", (5, pytds.output(param_type=int)))
    if cur.return_value != 1:
        print(f"FAIL: positive RETURN status: got {cur.return_value}, want 1")
        return 1

    conn.close()
    print("tds procs conformance: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
