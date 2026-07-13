#!/usr/bin/env python3
"""TDS TLS conformance smoke: connect over the tunneled-TLS handshake with an
independent TDS client (python-tds), then run a create/insert/select round trip.

Usage: tds_conformance_tls.py <host> <port> <user> <password> <ca_pem>
Exits non-zero on any mismatch.
"""
import sys

import pytds


def main() -> int:
    host, port, user, password, ca = (
        sys.argv[1],
        int(sys.argv[2]),
        sys.argv[3],
        sys.argv[4],
        sys.argv[5],
    )

    conn = pytds.connect(
        host,
        "truthdb",
        user,
        password,
        port=port,
        login_timeout=10,
        autocommit=True,
        cafile=ca,
        validate_host=False,
        disable_connect_retry=True,
    )
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE tls_conf (id INT NOT NULL PRIMARY KEY, name NVARCHAR(20))"
    )
    cur.execute("INSERT INTO tls_conf VALUES (1, 'encrypted'), (2, 'session')")
    cur.execute("SELECT id, name FROM tls_conf ORDER BY id")
    rows = cur.fetchall()
    expected = [(1, "encrypted"), (2, "session")]
    if rows != expected:
        print(f"FAIL: TLS SELECT mismatch\n got: {rows}\n want: {expected}")
        return 1

    print("tds tls conformance: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
