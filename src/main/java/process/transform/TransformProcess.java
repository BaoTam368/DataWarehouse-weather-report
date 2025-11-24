package process.transform;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class TransformProcess {

    public void runTransform(int sourceId, List<String> transactionSqlPath) {
        // Thời điểm bắt đầu bước RUN
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());

        boolean success = false;

        try (
                Connection stagingConn = DataBase.connectDB("localhost", 3306, "root", "1234", "staging");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            // Kiểm tra kết nối database
            if (stagingConn == null || controlConn == null) {
                System.out.println("Kết nối DB staging/control thất bại");
                return;
            }

            // 1. VALIDATE SCHEMA -> process_code = TR
            boolean ready = isReady(sourceId, controlConn, validateStart);

            if (!ready) {
                System.out.println("Schema không đúng, dừng Transform.");
                return;
            }

            // 2. THỰC HIỆN TRANSFORM -> process_code = TO
            // Tắt auto commit để có thể rollback tránh hỏng schema do từng phần trong script chạy lẻ
            stagingConn.setAutoCommit(false);
            Timestamp transformStart = new Timestamp(System.currentTimeMillis());

            success = isSuccess(transactionSqlPath, stagingConn, success);

            // Thời điểm kết thúc RUN
            Timestamp transformEnd = new Timestamp(System.currentTimeMillis());

            // Ghi log TO (Transform Ongoing)
            extracted(sourceId, controlConn, success, transformStart, transformEnd);

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Transform");
        }
    }

    /**
     * Ghi log TO (Transform Ongoing)
     * @param sourceId     Nguồn dữ liệu (config_source.source_id)
     * @param controlConn  Kết nối DB control
     * @param success     Trạng thái của quá trình transform
     * @param transformStart  Thời điểm bắt đầu quá trình transform
     * @param transformEnd    Thời điểm kết thúc quá trình transform
     */
    private static void extracted(int sourceId, Connection controlConn, boolean success, Timestamp transformStart, Timestamp transformEnd) {
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "TO",
                "Run transform scripts",
                success ? "SC" : "F",                // SC / F
                transformStart,
                transformEnd
        );
    }

    /**
     * Kiêm tra xem quá trình transform có thành công hay không
     * @param transactionSqlPath danh sách các file .sql cần chạy trong quá trình transform
     * @param stagingConn kết nối DB staging
     * @param success trạng thái của quá trình transform
     * @return true nếu transform thành công, false ngược lại
     * @throws SQLException nếu có lỗi khi thực thi các file .sql
     */
    private boolean isSuccess(List<String> transactionSqlPath, Connection stagingConn, boolean success) throws SQLException {
        try {
            for (String path : transactionSqlPath) executeSqlScript(stagingConn, path);
            stagingConn.commit();
            success = true;
            System.out.println("Transform thành công!");
        } catch (Exception ex) {
            stagingConn.rollback();
            System.out.println("Transform thất bại");
        }
        return success;
    }

    /**
     * Check if transform is ready
     * @param sourceId     Nguồn dữ liệu (config_source.source_id)
     * @param controlConn  Kết nối DB control
     * @param validateStart  Thời điểm bắt đầu validate
     * @return true nếu transform đã sẵn sàng, false ngược lại
     */
    private static boolean isReady(int sourceId, Connection controlConn, Timestamp validateStart) {
        TransformValidator validator = new TransformValidator();
        boolean ready = validator.validateAll();

        Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

        // Ghi log bước TR (Transform Ready)
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "TR",
                "Validate schema before transform",  // process_name
                ready ? "SC" : "F",                  // SC = OK, F = Fail
                validateStart,
                validateEnd
        );
        return ready;
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