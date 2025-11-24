package process.mart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class MartValidator {

    private static final String DB_MART = "mart_weather";
    private static final String DB_WAREHOUSE = "datawarehouse";

    /**
     * Validate schema cho mart:
     *  1) datawarehouse.aggregate_weather_daily
     *  2) mart_weather.WeatherDailySummary
     */
    public boolean validateAll(Connection martConn, Connection warehouseConn) {

        if (martConn == null || warehouseConn == null) {
            System.out.println("❌ Không kết nối được DB mart_weather hoặc datawarehouse để validate");
            return false;
        }

        boolean ok = true;

        try {
            ok &= validateWarehouseAggregate(warehouseConn);
            ok &= validateMartWeatherSummary(martConn);
        } catch (Exception e) {
            System.out.println("❌ Lỗi khi validate schema Mart: " + e.getMessage());
            e.printStackTrace();
            ok = false;
        }

        if (ok) {
            System.out.println("✅ Mart Ready (MR): Schema warehouse + mart_weather OK");
        } else {
            System.out.println("❌ Mart NOT Ready: thiếu cột/bảng.");
        }

        return ok;
    }

    // ===== Validate bảng aggregate_weather_daily trong datawarehouse =====
    private boolean validateWarehouseAggregate(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DATEONLY", "AVGTEMP", "MINTEMP", "MAXTEMP",
                "AVGHUMIDITY", "AVGPRESSURE"
        );
        return validateColumns(conn, DB_WAREHOUSE, "aggregate_weather_daily", requiredCols);
    }

    // ===== Validate bảng WeatherDailySummary trong mart_weather =====
    private boolean validateMartWeatherSummary(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DATEONLY", "AVGTEMP", "MINTEMP", "MAXTEMP",
                "AVGHUMIDITY", "AVGPRESSURE", "TEMPCATEGORY"
        );
        return validateColumns(conn, DB_MART, "WeatherDailySummary", requiredCols);
    }

    // ===== Generic: Kiểm tra bảng có đủ cột chưa =====
    private boolean validateColumns(Connection conn, String dbName, String tableName,
                                    List<String> requiredColumns) throws Exception {

        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, dbName);
        ps.setString(2, tableName);

        ResultSet rs = ps.executeQuery();

        Set<String> existing = new HashSet<>();
        while (rs.next()) {
            existing.add(rs.getString("COLUMN_NAME").toUpperCase());
        }

        boolean ok = true;
        for (String col : requiredColumns) {
            if (!existing.contains(col.toUpperCase())) {
                System.out.println("❌ [" + dbName + "." + tableName + "] thiếu cột: " + col);
                ok = false;
            }
        }

        if (ok) {
            System.out.println("✅ [" + dbName + "." + tableName + "] đầy đủ cột cần thiết.");
        }

        return ok;
    }
}