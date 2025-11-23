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
        Timestamp startTime = new Timestamp(System.currentTimeMillis());
        boolean success = false;

        try (
                Connection martConn = DataBase.connectDB("localhost", 3306, "root", "1234", "mart_weather");
                Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")
        ) {
            // Kết nối DB mart_weather
            if (martConn != null && controlConn != null) {
                martConn.setAutoCommit(false);

                try {
                    for (String path : martSqlPath) {
                        executeSqlScript(martConn, path);
                    }
                    martConn.commit();
                    success = true;
                    System.out.println("load mart thành công!");
                } catch (Exception ex) {
                    martConn.rollback();
                    System.out.println("load mart thất bại!");
                    ex.printStackTrace();
                }

                // Ghi log vào process_log sau khi chạy xong
                Timestamp endTime = new Timestamp(System.currentTimeMillis());
                String status = success ? "SUCCESS" : "FAILED";

                Control.insertProcessLog(
                        controlConn,
                        sourceId,
                        "LOAD_MART",                      // process_code
                        "Load dữ liệu vào mart_weather",  // process_name
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