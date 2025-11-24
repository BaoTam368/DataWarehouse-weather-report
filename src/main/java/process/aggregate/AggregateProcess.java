package process.aggregate;

import database.Control;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class AggregateProcess {

    /**
     * Chạy quy trình Aggregate:
     * 1. Validate schema (AR)
     * 2. Nếu OK -> chạy danh sách script aggregate (AO)
     *
     * @param sourceId         config_source.source_id
     * @param aggregateSqlPath danh sách đường dẫn file .sql cần chạy
     * @param warehouseConn    kết nối DB warehouse (đã connect sẵn)
     * @param controlConn      kết nối DB control (đã connect sẵn)
     */
    public void runAggregate(int sourceId,
                             List<String> aggregateSqlPath,
                             Connection warehouseConn,
                             Connection controlConn) {

        // Thời điểm bắt đầu bước validate
        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success;

        try (warehouseConn; controlConn) {
            // Kiểm tra kết nối database
            if (warehouseConn == null || controlConn == null) {
                System.out.println("Kết nối DB warehouse/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> process_code = AR
            AggregateValidator validator = new AggregateValidator();
            boolean ready = validator.validateAll(warehouseConn);

            Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

            // Ghi log AR (Aggregate Ready)
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

            // 2. THỰC HIỆN AGGREGATE -> process_code = AO
            warehouseConn.setAutoCommit(false);
            Timestamp aggregateStart = new Timestamp(System.currentTimeMillis());

            success = executeAggregateScripts(aggregateSqlPath, warehouseConn);

            Timestamp aggregateEnd = new Timestamp(System.currentTimeMillis());

            // Ghi log AO (Aggregate Ongoing / Done)
            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "AO",                               // Aggregate Ongoing
                    "Aggregate weather daily",          // process_name
                    success ? "SC" : "F",               // SC / F
                    aggregateStart,
                    aggregateEnd
            );

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Aggregate: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Thực hiện danh sách script .sql để aggregate
     */
    private boolean executeAggregateScripts(List<String> aggregateSqlPath,
                                            Connection warehouseConn) throws SQLException {
        boolean success = false;
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

    /**
     * Dùng MyBatis ScriptRunner để chạy 1 file .sql
     */
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