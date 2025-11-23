package process.transform;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;

public class TransformProcess {

    public void runTransform(int sourceId, List<String> transactionSqlPath) {
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection stagingConn = DataBase.connectDB("localhost", 3306, "root", "1234", "staging");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            if (stagingConn != null && controlConn != null) {
                stagingConn.setAutoCommit(false);

                try {
                    for (String path : transactionSqlPath) {
                        executeSqlScript(stagingConn, path);
                    }
                    stagingConn.commit();
                    success = true;
                    System.out.println("Transform thành công!");
                } catch (Exception ex) {
                    stagingConn.rollback();
                    System.out.println("Transform thất bại");
                }

                // Sau khi chạy xong (thành công hoặc thất bại) -> ghi log
                Timestamp endTime = new Timestamp(System.currentTimeMillis());
                String status = success ? "SUCCESS" : "FAILED";

                Control.insertProcessLog(
                        controlConn,
                        sourceId,
                        "TRANSFORM",                  // process_code
                        "Run transform scripts",      // process_name
                        status,
                        startTime,
                        endTime
                );
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