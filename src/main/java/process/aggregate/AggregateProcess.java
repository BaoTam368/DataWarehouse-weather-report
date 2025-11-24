package process.aggregate;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class AggregateProcess {

    public void runAggregate(int sourceId, List<String> aggregateSqlPath) {

        // Thời điểm bắt đầu run
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection warehouseConn = DataBase.connectDB("localhost", 3306, "root", "1234", "warehouse");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            // Kiểm tra kết nối database
            if (warehouseConn == null || controlConn == null) {
                System.out.println("Kết nối DB warehouse/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> process_code = AR
            boolean ready = isReady(sourceId, controlConn, validateStart);

            if (!ready) {
                System.out.println("Schema warehouse không đúng, dừng Aggregate.");
                return;
            }

            // 2. THỰC HIỆN AGGREGATE -> process_code = AG
            // Tắt auto commit để có thể rollback tránh hỏng schema do từng phần trong script chạy lẻ
            warehouseConn.setAutoCommit(false);
            Timestamp aggregateStart = new Timestamp(System.currentTimeMillis());

            success = isSuccess(aggregateSqlPath, warehouseConn, success);

            // Thời điểm kết thúc run
            Timestamp aggregateEnd = new Timestamp(System.currentTimeMillis());

            extracted(sourceId, controlConn, success, aggregateStart, aggregateEnd);

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Aggregate!");
        }
    }

    private static void extracted(int sourceId, Connection controlConn, boolean success, Timestamp aggregateStart, Timestamp aggregateEnd) {
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "AO",                               // Aggregate ongoing
                "Aggregate weather daily",          // process_name
                success ? "SC" : "F",               // SC / F
                aggregateStart,
                aggregateEnd
        );
    }

    private boolean isSuccess(List<String> aggregateSqlPath, Connection warehouseConn, boolean success) throws SQLException {
        try {
            for (String path : aggregateSqlPath) {
                executeSqlScript(warehouseConn, path);
            }
            warehouseConn.commit();
            success = true;
            System.out.println("Aggregate weather daily thành công!");
        } catch (Exception ex) {
            warehouseConn.rollback();
            System.out.println("Aggregate weather daily thất bại!");
            System.out.println("Chi tiết lỗi khi aggregate: " + ex.getMessage());
            ex.printStackTrace();
        }
        return success;
    }

    private static boolean isReady(int sourceId, Connection controlConn, Timestamp validateStart) {
        AggregateValidator validator = new AggregateValidator();
        boolean ready = validator.validateAll();

        Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

        Control.insertProcessLog(
                controlConn,
                sourceId,
                "AR",                               // Aggregate Ready
                "Validate schema before aggregate", // process_name
                ready ? "SC" : "F",                 // SC = OK, F = fail
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