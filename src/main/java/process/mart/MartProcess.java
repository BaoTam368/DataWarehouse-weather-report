package process.mart;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;

public class MartProcess {

    public void runMart(int sourceId, List<String> martSqlPath) {
        // Bắt đầu bước validate
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection martConn = DataBase.connectDB("localhost", 3306, "root", "1234", "mart_weather");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            if (martConn == null || controlConn == null) {
                System.out.println("Kết nối DB mart_weather/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> MR (Mart Ready)
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

            if (!ready) {
                System.out.println("Schema không đúng, dừng load mart.");
                return;
            }

            // 2. THỰC HIỆN LOAD MART -> LM (Load Mart)
            martConn.setAutoCommit(false);
            Timestamp loadStart = new Timestamp(System.currentTimeMillis());

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
            }

            Timestamp loadEnd = new Timestamp(System.currentTimeMillis());

            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "LM",                               // Load Mart
                    "Load dữ liệu vào mart_weather",    // process_name
                    success ? "SC" : "F",
                    loadStart,
                    loadEnd
            );

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy MartProcess!");
        }
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