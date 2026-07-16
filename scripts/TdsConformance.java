// TDS conformance: mssql-jdbc, the third independent driver — and the one
// whose default prepared-statement flow exercises what pytds and go-mssqldb
// cannot: sp_prepexec on first execution, sp_execute on re-use, and
// discarded-handle sp_unprepare calls BATCHED into the next execution's
// request (a multi-RPC request), all under DONEPROC/RETURNSTATUS framing.
//
// Usage: java -cp mssql-jdbc.jar TdsConformance.java <host> <port> <user> <password>
// Exits non-zero on any mismatch.

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TdsConformance {
    static void fail(String message) {
        System.err.println("FAIL: " + message);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        String url = String.format(
                "jdbc:sqlserver://%s:%s;databaseName=truthdb;encrypt=false;user=%s;password=%s;loginTimeout=10",
                args[0], args[1], args[2], args[3]);
        try (Connection conn = DriverManager.getConnection(url)) {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate("CREATE TABLE prep_jdbc (id INT NOT NULL PRIMARY KEY, name NVARCHAR(40))");
            }

            // A prepared INSERT executed repeatedly: sp_prepexec on the first
            // execution, sp_execute after — and each execution past the
            // driver's discard threshold batches sp_unprepare RPCs in front.
            try (PreparedStatement ins = conn.prepareStatement("INSERT INTO prep_jdbc VALUES (?, ?)")) {
                for (int i = 1; i <= 5; i++) {
                    ins.setInt(1, i);
                    ins.setString(2, "row" + i);
                    if (ins.executeUpdate() != 1) {
                        fail("insert " + i + ": rows affected != 1");
                    }
                }
            }

            // A prepared SELECT re-used with different parameters.
            try (PreparedStatement sel = conn.prepareStatement("SELECT name FROM prep_jdbc WHERE id = ?")) {
                for (int i = 1; i <= 5; i++) {
                    sel.setInt(1, i);
                    try (ResultSet rs = sel.executeQuery()) {
                        if (!rs.next() || !rs.getString(1).equals("row" + i)) {
                            fail("select id=" + i + " mismatch");
                        }
                    }
                }

                // DDL between executions: there is no cached plan to go
                // stale — the same handle sees the new row.
                try (Statement st = conn.createStatement()) {
                    st.executeUpdate("INSERT INTO prep_jdbc VALUES (6, 'row6')");
                }
                sel.setInt(1, 6);
                try (ResultSet rs = sel.executeQuery()) {
                    if (!rs.next() || !rs.getString(1).equals("row6")) {
                        fail("select after insert mismatch");
                    }
                }
            }

            // An error inside a prepared execution surfaces as a SQLException
            // with the server's number, and the connection stays usable.
            try (PreparedStatement dup = conn.prepareStatement("INSERT INTO prep_jdbc VALUES (?, ?)")) {
                dup.setInt(1, 1);
                dup.setString(2, "dup");
                dup.executeUpdate();
                fail("duplicate PK did not raise");
            } catch (SQLException e) {
                if (e.getErrorCode() != 2627) {
                    fail("expected 2627, got " + e.getErrorCode() + ": " + e.getMessage());
                }
            }
            try (Statement st = conn.createStatement();
                    ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM prep_jdbc")) {
                rs.next();
                if (rs.getInt(1) != 6) {
                    fail("count: got " + rs.getInt(1) + " want 6");
                }
            }
        }
        System.out.println("tds conformance (mssql-jdbc): OK");
    }
}
