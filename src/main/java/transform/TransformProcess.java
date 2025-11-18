package transform;

import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;

public class TransformProcess {

    public void runTransform(String transactionSqlPath) {
        Connection conn = null;
        try {
            conn = DataBase.connectDB("localhost", 3306, "root", "1234", "staging");
            conn.setAutoCommit(false);

            try {
                executeSqlScript(conn, transactionSqlPath);

                conn.commit();
                System.out.println("Transform thành công!");
            } catch (Exception ex) {
                conn.rollback();
                ex.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
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