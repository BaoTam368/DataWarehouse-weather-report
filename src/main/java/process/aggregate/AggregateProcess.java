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

        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection warehouseConn = DataBase.connectDB("localhost", 3306, "root", "1234", "warehouse");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            // Kết nối DB warehouse
            if (warehouseConn != null && controlConn != null) {
                warehouseConn.setAutoCommit(false);

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
                    ex.printStackTrace();
                }

                // Ghi log vào process_log
                Timestamp endTime = new Timestamp(System.currentTimeMillis());
                String status = success ? "SUCCESS" : "FAILED";

                Control.insertProcessLog(
                        controlConn,
                        sourceId,
                        "AGGREGATE",                        // process_code
                        "Aggregate weather daily",          // process_name
                        status,
                        startTime,
                        endTime
                );
            }
        } catch (Exception e) {
            System.out.println("Kết nối thất bại!");
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