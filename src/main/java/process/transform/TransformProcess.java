package process.transform;

import database.DBConnection;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;

public class TransformProcess {

    public void runTransform(List<String> transactionSqlPath) {
        try (Connection conn = DBConnection.connectDB("localhost", 3306, "root", "1234", "staging")) {
            // Kết nối DB staging
            if (conn != null) {
                conn.setAutoCommit(false);

                try {
                    for (String path : transactionSqlPath) {
                        executeSqlScript(conn, path);
                    }
                    conn.commit();
                    System.out.println("Transform thành công!");
                } catch (Exception ex) {
                    conn.rollback();
                    System.out.println("Transform thất bại");
                }
            }
        } catch (Exception e) {
            System.out.println("Kết nối thất bại");
        }
    }

    /**
     * Dùng MyBatis ScriptRunner để chạy script .sql
     */
    private void executeSqlScript(Connection conn, String filePath) throws Exception {
        ScriptRunner runner = new ScriptRunner(conn);

        runner.setSendFullScript(false);
        runner.setLogWriter(null);          // tắt log chi tiết ra console
        runner.setErrorLogWriter(null);

        try (Reader reader = new FileReader(filePath)) {
            runner.runScript(reader);
        }
    }
}