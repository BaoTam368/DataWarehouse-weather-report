package process.aggregate;

import database.Control;
import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;

public class AggregateProcess {

    public void runAggregate(int sourceId, List<String> aggregateSqlPath) {

        // Thời điểm bắt đầu bước VALIDATE (AR)
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection warehouseConn = DataBase.connectDB("localhost", 3306, "root", "1234", "warehouse");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            if (warehouseConn == null || controlConn == null) {
                System.out.println("Kết nối DB warehouse/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> process_code = AR
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

            if (!ready) {
                System.out.println("Schema warehouse không đúng, dừng Aggregate.");
                return;
            }

            // 2. THỰC HIỆN AGGREGATE -> process_code = AG
            warehouseConn.setAutoCommit(false);
            Timestamp aggregateStart = new Timestamp(System.currentTimeMillis());

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
            }

            Timestamp aggregateEnd = new Timestamp(System.currentTimeMillis());

            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "AO",                               // Aggregate ongoing
                    "Aggregate weather daily",          // process_name
                    success ? "SC" : "F",               // SC / F
                    aggregateStart,
                    aggregateEnd
            );

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Aggregate!");
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