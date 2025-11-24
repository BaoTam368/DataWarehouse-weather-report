package process.mart;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class MartProcess {

    public void runMart(int sourceId, List<String> martSqlPath) {
        // Thời điểm bắt đầu run
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection martConn = DataBase.connectDB("localhost", 3306, "root", "1234", "mart_weather");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            // Kiểm tra kết nối database
            if (martConn == null || controlConn == null) {
                System.out.println("Kết nối DB mart_weather/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> MR (Mart Ready)
            boolean ready = isReady(sourceId, controlConn, validateStart);

            if (!ready) {
                System.out.println("Schema không đúng, dừng load mart.");
                return;
            }

            // 2. THỰC HIỆN LOAD MART -> LM (Load Mart)
            // Tắt auto commit để có thể rollback tránh hỏng schema do từng phần trong script chạy lẻ
            martConn.setAutoCommit(false);
            Timestamp loadStart = new Timestamp(System.currentTimeMillis());

            success = isSuccess(martSqlPath, martConn, success);

            // Thời điểm kết thúc run
            Timestamp loadEnd = new Timestamp(System.currentTimeMillis());

            extracted(sourceId, controlConn, success, loadStart, loadEnd);

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy MartProcess!");
        }
    }

    private static void extracted(int sourceId, Connection controlConn, boolean success, Timestamp loadStart, Timestamp loadEnd) {
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "LM",                               // Load Mart
                "Load dữ liệu vào mart_weather",    // process_name
                success ? "SC" : "F",
                loadStart,
                loadEnd
        );
    }

    private boolean isSuccess(List<String> martSqlPath, Connection martConn, boolean success) throws SQLException {
        try {
            for (String path : martSqlPath) {
                executeSqlScript(martConn, path);
            }
            martConn.commit();
            success = true;
            System.out.println("Load mart thành công!");
        } catch (Exception ex) {
            martConn.rollback();
            System.out.println("Load mart thất bại!");
            System.out.println("Chi tiết lỗi khi load mart: " + ex.getMessage());
        }
        return success;
    }

    private static boolean isReady(int sourceId, Connection controlConn, Timestamp validateStart) {
        MartValidator validator = new MartValidator();
        boolean ready = validator.validateAll();

        Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

        Control.insertProcessLog(
                controlConn,
                sourceId,
                "MR",                               // Mart Ready
                "Validate schema before load mart", // process_name
                ready ? "SC" : "F",                 // SC = success, F = fail
                validateStart,
                validateEnd
        );
        return ready;
    }

    private void executeSqlScript(Connection conn, String filePath) throws Exception {
        ScriptRunner runner = new ScriptRunner(conn);
        runner.setSendFullScript(false);
        runner.setLogWriter(null);
        runner.setErrorLogWriter(null);

        try (Reader reader = new FileReader(filePath)) {
            runner.runScript(reader);
        }
    }
}