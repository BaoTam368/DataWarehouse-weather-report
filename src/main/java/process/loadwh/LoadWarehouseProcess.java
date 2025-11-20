package process.loadwh;

import database.DBConnection;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;

public class LoadWarehouseProcess {

    public void runLoadWarehouse(List<String> sqlFiles) {
        try (Connection conn = DBConnection.connectDB("localhost", 3306, "root", "123456", "datawarehouse")) {
            if (conn == null) {
                System.out.println("❌ Không thể kết nối DB datawarehouse!");
                return;
            }
            conn.setAutoCommit(false);
            System.out.println("🔗 Kết nối thành công vào datawarehouse.");

            try {
                for (String path : sqlFiles) {
                    executeSqlScript(conn, path);
                }
                conn.commit();
                System.out.println("✅ Load warehouse thành công!");

            } catch (Exception ex) {
                conn.rollback();
                System.out.println("❌ Load warehouse thất bại!");
                ex.printStackTrace();
            }

        } catch (Exception e) {
            System.out.println("❌ Không thể kết nối DB!");
        }
    }

    private void executeSqlScript(Connection conn, String filePath) throws Exception {

        ScriptRunner runner = new ScriptRunner(conn);
        runner.setSendFullScript(false);
        runner.setLogWriter(null);
        runner.setErrorLogWriter(null);

        System.out.println("▶ Running SQL: " + filePath);

        try (Reader reader = new FileReader(filePath)) {
            runner.runScript(reader);
        }

        System.out.println("✔ Completed: " + filePath);
    }
}
