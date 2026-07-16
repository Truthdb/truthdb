#!/usr/bin/env python3
"""The five-isolation-level matrix over a real TDS driver (pytds), two
connections — Stage 13's exit demo. Each level is pinned by a value only its
semantics can produce:

  READ UNCOMMITTED  reader sees an uncommitted write (dirty read)
  READ COMMITTED    reader waits for the writer's commit (lock-based)
  RCSI              reader returns the PRE-update value while the writer's
                    transaction is open (no wait — the whole point)
  REPEATABLE READ   reader waits like RC (and RCSI must NOT apply to it)
  SNAPSHOT          repeatable reads across the writer's commit, 3960 on a
                    write conflict, 3952 when the option is off

Usage: tds_isolation_matrix.py <host> <port> <user> <password>
Exits non-zero on any mismatch.
"""

import sys
import threading

import pytds


def connect(host, port, user, password, autocommit=True):
    return pytds.connect(
        host, "truthdb", user, password, port=port, login_timeout=10, autocommit=autocommit
    )


def error_number(exc):
    number = getattr(exc, "number", None)
    if number is not None:
        return number
    # pytds surfaces some server errors only through the message text.
    text = str(exc)
    for candidate in (3960, 3952, 3961, 1222, 1205):
        if str(candidate) in text:
            return candidate
    return None


def fetch_v(cur, conn_label):
    cur.execute("SELECT v FROM iso WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        raise AssertionError(f"{conn_label}: no row for id = 1")
    return row[0]


def main() -> int:
    host, port, user, password = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

    admin = connect(host, port, user, password)
    setup = admin.cursor()
    setup.execute("CREATE TABLE iso (id INT NOT NULL PRIMARY KEY, v INT)")
    setup.execute("INSERT INTO iso VALUES (1, 10), (2, 20)")

    # ---- READ UNCOMMITTED: the dirty read --------------------------------
    writer = connect(host, port, user, password, autocommit=False)
    wcur = writer.cursor()
    wcur.execute("UPDATE iso SET v = 99 WHERE id = 1")
    ru = connect(host, port, user, password)
    rcur = ru.cursor()
    rcur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    seen = fetch_v(rcur, "RU reader")
    if seen != 99:
        print(f"FAIL: READ UNCOMMITTED must dirty-read 99, got {seen}")
        return 1
    writer.rollback()
    ru.close()

    # ---- READ COMMITTED (lock-based): the reader waits -------------------
    # The reader is started while the writer holds its X lock; it can only
    # ever return the committed value, because it parks until the commit.
    writer2 = connect(host, port, user, password, autocommit=False)
    wcur = writer2.cursor()
    wcur.execute("UPDATE iso SET v = 30 WHERE id = 1")
    result = {}

    def rc_read():
        rc = connect(host, port, user, password)
        cur = rc.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        result["rc"] = fetch_v(cur, "RC reader")
        rc.close()

    t = threading.Thread(target=rc_read)
    t.start()
    t.join(timeout=1.0)
    if not t.is_alive() and result.get("rc") == 10:
        print("FAIL: lock-based READ COMMITTED returned the pre-update value "
              "without waiting for the writer")
        return 1
    writer2.commit()
    t.join(timeout=10)
    if result.get("rc") != 30:
        print(f"FAIL: READ COMMITTED must see only the commit (30), got {result.get('rc')}")
        return 1

    # ---- RCSI: the reader does not wait and sees the pre-update value ----
    setup.execute("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON")
    writer3 = connect(host, port, user, password, autocommit=False)
    wcur = writer3.cursor()
    wcur.execute("UPDATE iso SET v = 55 WHERE id = 1")
    rcsi = connect(host, port, user, password)
    cur = rcsi.cursor()
    seen = fetch_v(cur, "RCSI reader")
    if seen != 30:
        print(f"FAIL: RCSI reader must see the committed 30 while the writer "
              f"holds its lock, got {seen}")
        return 1
    writer3.commit()
    seen = fetch_v(cur, "RCSI reader after commit")
    if seen != 55:
        print(f"FAIL: RCSI reader must see 55 after the commit, got {seen}")
        return 1
    rcsi.close()

    # ---- REPEATABLE READ: still lock-based, RCSI does not apply ----------
    writer4 = connect(host, port, user, password, autocommit=False)
    wcur = writer4.cursor()
    wcur.execute("UPDATE iso SET v = 60 WHERE id = 1")
    result = {}

    def rr_read():
        rr = connect(host, port, user, password)
        cur = rr.cursor()
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        result["rr"] = fetch_v(cur, "RR reader")
        rr.close()

    t = threading.Thread(target=rr_read)
    t.start()
    t.join(timeout=1.0)
    if not t.is_alive() and result.get("rr") == 55:
        print("FAIL: REPEATABLE READ returned the pre-update value without waiting")
        return 1
    writer4.commit()
    t.join(timeout=10)
    if result.get("rr") != 60:
        print(f"FAIL: REPEATABLE READ must see only the commit (60), got {result.get('rr')}")
        return 1

    # ---- SNAPSHOT: 3952 while off, then repeatable + 3960 conflict -------
    si = connect(host, port, user, password, autocommit=False)
    scur = si.cursor()
    scur.execute("SET TRANSACTION ISOLATION LEVEL SNAPSHOT")
    try:
        fetch_v(scur, "SI reader (option off)")
        print("FAIL: SNAPSHOT access without ALLOW_SNAPSHOT_ISOLATION must be 3952")
        return 1
    except pytds.Error as exc:
        if error_number(exc) != 3952:
            print(f"FAIL: expected 3952, got {exc!r}")
            return 1
    si.rollback()

    setup.execute("ALTER DATABASE CURRENT SET ALLOW_SNAPSHOT_ISOLATION ON")
    seen = fetch_v(scur, "SI reader first access")
    if seen != 60:
        print(f"FAIL: SNAPSHOT first read must see 60, got {seen}")
        return 1
    # A committed writer change after the snapshot stays invisible...
    setup.execute("UPDATE iso SET v = 70 WHERE id = 1")
    seen = fetch_v(scur, "SI reader after external commit")
    if seen != 60:
        print(f"FAIL: SNAPSHOT read must stay at 60 (repeatable), got {seen}")
        return 1
    # ...and writing that row is the classic 3960 update conflict, which
    # rolls the transaction back entirely.
    try:
        scur.execute("UPDATE iso SET v = 61 WHERE id = 1")
        print("FAIL: SNAPSHOT update conflict did not raise")
        return 1
    except pytds.Error as exc:
        if error_number(exc) != 3960:
            print(f"FAIL: expected 3960, got {exc!r}")
            return 1
    # The server rolled the transaction back on the conflict; the driver
    # still believes one is open, so its own rollback-on-close may error.
    try:
        si.close()
    except pytds.Error:
        pass

    check = connect(host, port, user, password)
    ccur = check.cursor()
    seen = fetch_v(ccur, "post-matrix check")
    if seen != 70:
        print(f"FAIL: the conflicting write must not have landed; want 70, got {seen}")
        return 1

    # Leave the database as the other conformance scripts expect it.
    ccur.execute("ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF, "
                 "ALLOW_SNAPSHOT_ISOLATION OFF")
    ccur.execute("DROP TABLE iso")
    check.close()
    admin.close()

    print("tds isolation matrix: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
