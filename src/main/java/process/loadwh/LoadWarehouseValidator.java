package process.loadwh;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LoadWarehouseValidator {

    // Validate toàn bộ
    public boolean validateAll(
            Connection warehouseConn,
            Connection stagingConn,
            String procName,
            String warehouseScript,
            String countSqlScript) {

        try {

            boolean dbOk = warehouseConn != null && stagingConn != null;
            if (!dbOk) {
                System.out.println("❌ Không kết nối được DB warehouse hoặc staging.");
                return false;
            }

            boolean procOk = checkProcedureExists(warehouseConn, procName);
            boolean tablesOk = validateDimFactTables(warehouseConn);
            boolean fileOk = validateFiles(warehouseScript, countSqlScript);
            boolean stagingOk = checkStagingHasRows(stagingConn);

            if (procOk && tablesOk && fileOk && stagingOk) {
                System.out.println("✅ LoadToWarehouse Ready.");
                return true;
            }

            System.out.println("❌ LoadToWarehouse không đủ điều kiện.");
            return false;

        } catch (Exception e) {
            System.out.println("❌ Lỗi validate LoadToWarehouse: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // ============== check procedure ====================
    private boolean checkProcedureExists(Connection conn, String procName) throws Exception {
        String sql = "SHOW PROCEDURE STATUS WHERE Name = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, procName);
        ResultSet rs = ps.executeQuery();
        boolean exists = rs.next();

        System.out.println(exists ?
                "✅ PROC tồn tại: " + procName
                :
                "❌ PROC không tồn tại: " + procName);

        return exists;
    }

    // ============== check tables ====================
    private boolean validateDimFactTables(Connection conn) throws Exception {
        String[] tables = {"DimTime", "DimWind", "DimUV", "FactWeather"};

        boolean ok = true;
        for (String t : tables) {
            if (!checkTableExists(conn, t)) {
                System.out.println("❌ Thiếu bảng: " + t);
                ok = false;
            } else {
                System.out.println("✅ Bảng OK: " + t);
            }
        }
        return ok;
    }

    private boolean checkTableExists(Connection conn, String table) throws Exception {
        String sql = "SHOW TABLES LIKE ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, table);
        ResultSet rs = ps.executeQuery();
        return rs.next();
    }

    // ============== check files ====================
    private boolean validateFiles(String... paths) {
        boolean ok = true;

        for (String p : paths) {
            if (p == null) continue;

            File f = new File(p);
            if (!f.exists()) {
                System.out.println("❌ File không tồn tại: " + p);
                ok = false;
            } else {
                System.out.println("✅ File OK: " + p);
            }
        }
        return ok;
    }

    // ============== check staging_clean ====================
    private boolean checkStagingHasRows(Connection stagingConn) throws Exception {
        String sql = "SELECT COUNT(*) AS total FROM staging.stg_weather_clean";
        PreparedStatement ps = stagingConn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        int total = 0;
        if (rs.next()) total = rs.getInt("total");

        if (total > 0) {
            System.out.println("✅ staging_clean có " + total + " dòng.");
            return true;
        }

        System.out.println("❌ staging_clean KHÔNG CÓ dữ liệu.");
        return false;
    }
}

