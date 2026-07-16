#!/usr/bin/env python3
"""TDS conformance: the sp_prepare handle family over real RPC.

pytds's `callproc` sends an RPC by procedure name with typed parameters and
fills `pytds.output(...)` arguments from the response's RETURNVALUE tokens —
so this adjudicates the whole family against an independent client: the
handle comes back through RETURNVALUE, sp_execute binds unnamed wire values
to the prepared declaration list, sp_unprepare drops the handle, and a
dropped/unknown handle answers 8179. (go-mssqldb cannot cover this:
database/sql's Prepare is client-side text caching — it re-sends
sp_executesql per execution and never speaks the handle family, verified by
running it against a server without the family.)

Usage: tds_conformance_prepared.py <host> <port> <user> <password>
Exits non-zero on any mismatch.
"""

import sys

import pytds


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
    conn = pytds.connect(
        host, "truthdb", user, password, port=port, login_timeout=10, autocommit=True
    )
    cur = conn.cursor()

    cur.execute(
        "CREATE TABLE prep_py (id INT NOT NULL PRIMARY KEY, name NVARCHAR(40))"
    )
    cur.execute("INSERT INTO prep_py VALUES (1, 'one'), (2, 'two'), (3, 'three')")

    # sp_prepare: the handle arrives as a RETURNVALUE-filled output parameter.
    result = cur.callproc(
        "sp_prepare",
        (
            pytds.output(param_type=int),
            "@p1 int",
            "SELECT name FROM prep_py WHERE id = @p1",
            1,
        ),
    )
    handle = result[0]
    if not isinstance(handle, int):
        fail(f"sp_prepare returned no integer handle: {result!r}")

    # sp_execute re-uses the handle; the value parameter is unnamed on the
    # wire and binds to the declaration's @p1.
    for wanted_id, wanted_name in ((2, "two"), (3, "three")):
        cur.callproc("sp_execute", (handle, wanted_id))
        rows = cur.fetchall()
        if rows != [(wanted_name,)]:
            fail(f"sp_execute id={wanted_id}: got {rows!r} want [({wanted_name!r},)]")

    # A write between executions is visible to the next execute: there is no
    # cached plan or snapshot behind the handle.
    cur.execute("INSERT INTO prep_py VALUES (4, 'four')")
    cur.callproc("sp_execute", (handle, 4))
    rows = cur.fetchall()
    if rows != [("four",)]:
        fail(f"sp_execute after insert: got {rows!r}")

    # sp_unprepare drops the handle; executing it afterwards answers 8179
    # (SQL Server's "Could not find prepared statement with handle %d").
    cur.callproc("sp_unprepare", (handle,))
    try:
        cur.callproc("sp_execute", (handle, 1))
        cur.fetchall()
        fail("sp_execute on a dropped handle did not raise")
    except pytds.Error as err:
        number = getattr(err, "number", None)
        if number != 8179:
            fail(f"dropped handle: expected 8179, got {number} ({err})")

    # sp_prepexec: prepare + execute in one round trip. Its RETURNVALUE
    # follows the result set, so the output param is readable only after the
    # rows are drained (get_proc_outputs), unlike sp_prepare's.
    cur.callproc(
        "sp_prepexec",
        (
            pytds.output(param_type=int),
            "@p1 int",
            "SELECT id FROM prep_py WHERE id > @p1 ORDER BY id",
            2,
        ),
    )
    rows = cur.fetchall()
    if rows != [(3,), (4,)]:
        fail(f"sp_prepexec rowset: got {rows!r}")
    handle2 = cur.get_proc_outputs()[0]
    if not isinstance(handle2, int):
        fail(f"sp_prepexec returned no integer handle: {handle2!r}")
    cur.callproc("sp_execute", (handle2, 3))
    rows = cur.fetchall()
    if rows != [(4,)]:
        fail(f"sp_execute on prepexec handle: got {rows!r}")
    cur.callproc("sp_unprepare", (handle2,))

    # A syntax error surfaces at prepare time, and allocates nothing.
    try:
        cur.callproc(
            "sp_prepare", (pytds.output(param_type=int), "", "SELEC oops", 1)
        )
        fail("sp_prepare of invalid SQL did not raise")
    except pytds.Error:
        pass

    conn.close()
    print("tds conformance (sp_prepare family, pytds): OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
