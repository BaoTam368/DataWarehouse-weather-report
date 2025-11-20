package process.loadwh;

import database.DBConnection;

import java.sql.CallableStatement;
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

            // 2️⃣ GỌI Stored Procedure sau khi commit
            callProcedure(conn, "proc_load_warehouse");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    private void callProcedure(Connection conn, String procName) throws Exception {
        String sql = "{CALL " + procName + "()}";
        try (CallableStatement stmt = conn.prepareCall(sql)) {
            stmt.execute();
            System.out.println("✔ Procedure executed: " + procName);
        }
    }
}
