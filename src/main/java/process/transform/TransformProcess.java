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
        // Thời điểm bắt đầu bước VALIDATE
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());

        boolean success = false;

        try (
                Connection stagingConn = DataBase.connectDB("localhost", 3306, "root", "1234", "staging");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            if (stagingConn == null || controlConn == null) {
                System.out.println("Kết nối DB staging/control thất bại");
                return;
            }

            // 1. VALIDATE SCHEMA -> process_code = TR
            TransformValidator validator = new TransformValidator();
            boolean ready = validator.validateAll();

            Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

            // Ghi log bước TR (Transform Ready)
            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "TR",                                // Transform Ready
                    "Validate schema before transform",  // process_name
                    ready ? "SC" : "F",                  // SC = OK, F = Fail
                    validateStart,
                    validateEnd
            );

            if (!ready) {
                System.out.println("Schema không đúng, dừng Transform.");
                return;
            }

            // 2. THỰC HIỆN TRANSFORM -> process_code = TO
            stagingConn.setAutoCommit(false);
            Timestamp transformStart = new Timestamp(System.currentTimeMillis());

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

            Timestamp transformEnd = new Timestamp(System.currentTimeMillis());

            // Ghi log bước TO (Transform Ongoing / thực thi)
            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "TO",                                // Transform Ongoing
                    "Run transform scripts",
                    success ? "SC" : "F",                // SC / F
                    transformStart,
                    transformEnd
            );

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Transform");
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