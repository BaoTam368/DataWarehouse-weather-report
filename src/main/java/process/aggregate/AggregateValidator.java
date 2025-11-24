package process.aggregate;

import database.DataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class AggregateValidator {

    private static final String DB_NAME = "warehouse";

    public boolean validateAll() {
        try (Connection conn = DataBase.connectDB("localhost", 3306, "root", "1234", DB_NAME)) {
            if (conn == null) {
                System.out.println("❌ Không kết nối được DB warehouse để validate");
                return false;
            }

            boolean factOk = validateFactWeather(conn);
            boolean dimOk = validateDimTime(conn);
            boolean aggOk = validateAggregateTable(conn);

            if (factOk && dimOk && aggOk) {
                System.out.println("✅ Aggregate Ready (AR): Schema warehouse OK");
                return true;
            } else {
                System.out.println("❌ Aggregate NOT Ready: thiếu cột/bảng trong warehouse");
                return false;
            }

        } catch (Exception e) {
            System.out.println("❌ Lỗi khi validate schema");
            return false;
        }
    }

    private boolean validateFactWeather(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "TimeKey", "Temperature", "Humidity", "Pressure"
        );
        return validateColumns(conn, "FactWeather", requiredCols);
    }

    private boolean validateDimTime(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "TimeKey", "DateOnly"
        );
        return validateColumns(conn, "DimTime", requiredCols);
    }

    private boolean validateAggregateTable(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DateOnly", "AvgTemp", "MinTemp", "MaxTemp",
                "AvgHumidity", "AvgPressure", "RowCount"
        );
        return validateColumns(conn, "aggregate_weather_daily", requiredCols);
    }

    private boolean validateColumns(Connection conn, String tableName, List<String> requiredColumns) throws Exception {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, DB_NAME);
        ps.setString(2, tableName);
        ResultSet rs = ps.executeQuery();

        Set<String> existing = new HashSet<>();
        while (rs.next()) {
            existing.add(rs.getString("COLUMN_NAME").toLowerCase());
        }

        boolean ok = true;
        for (String col : requiredColumns) {
            if (!existing.contains(col.toLowerCase())) {
                System.out.println("❌ Bảng " + tableName + " thiếu cột: " + col);
                ok = false;
            }
        }

        if (ok) {
            System.out.println("✅ Bảng " + tableName + " đầy đủ các cột cần thiết.");
        }
        return ok;
    }
}