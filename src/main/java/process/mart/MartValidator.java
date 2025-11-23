package process.mart;

import database.DataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class MartValidator {

    private static final String DB_MART = "mart_weather";
    private static final String DB_WAREHOUSE = "warehouse";

    public boolean validateAll() {
        boolean ok = true;

        try (Connection martConn = DataBase.connectDB("localhost", 3306, "root", "1234", DB_MART);
             Connection whConn   = DataBase.connectDB("localhost", 3306, "root", "1234", DB_WAREHOUSE)) {

            if (martConn == null || whConn == null) {
                System.out.println("❌ Không kết nối được DB mart_weather hoặc warehouse để validate");
                return false;
            }

            ok &= validateWarehouseAggregate(whConn);
            ok &= validateMartWeatherSummary(martConn);

        } catch (Exception e) {
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

    private boolean validateWarehouseAggregate(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DateOnly", "AvgTemp", "MinTemp", "MaxTemp",
                "AvgHumidity", "AvgPressure"
        );
        return validateColumns(conn, DB_WAREHOUSE, "aggregate_weather_daily", requiredCols);
    }

    private boolean validateMartWeatherSummary(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DateOnly", "AvgTemp", "MinTemp", "MaxTemp",
                "AvgHumidity", "AvgPressure", "TempCategory"
        );
        return validateColumns(conn, DB_MART, "WeatherDailySummary", requiredCols);
    }

    private boolean validateColumns(Connection conn, String dbName, String tableName, List<String> requiredColumns) throws Exception {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, dbName);
        ps.setString(2, tableName);
        ResultSet rs = ps.executeQuery();

        Set<String> existing = new HashSet<>();
        while (rs.next()) {
            existing.add(rs.getString("COLUMN_NAME").toLowerCase());
        }

        boolean ok = true;
        for (String col : requiredColumns) {
            if (!existing.contains(col.toLowerCase())) {
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