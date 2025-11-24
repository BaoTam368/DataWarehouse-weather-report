package process.aggregate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class AggregateValidator {

    // ĐÚNG tên schema
    private static final String DB_NAME = "datawarehouse";

    /**
     * Validate toàn bộ schema cần cho bước Aggregate
     *   - FactWeather  (fact)
     *   - date_dim     (date dimension)
     *   - DimTime      (nếu bạn còn dùng ở step khác)
     *   - aggregate_weather_daily (bảng đích)
     */
    public boolean validateAll(Connection warehouseConn) {

        if (warehouseConn == null) {
            System.out.println("❌ Không kết nối được DB datawarehouse để validate");
            return false;
        }

        try {
            boolean factOk    = validateFactWeather(warehouseConn);
            boolean dateOk    = validateDateDim(warehouseConn);
            boolean dimTimeOk = validateDimTime(warehouseConn);   // vẫn còn dùng ở fact load
            boolean aggOk     = validateAggregateTable(warehouseConn);

            if (factOk && dateOk && dimTimeOk && aggOk) {
                System.out.println("✅ Aggregate Ready (AR): Schema datawarehouse OK");
                return true;
            } else {
                System.out.println("❌ Aggregate NOT Ready: thiếu cột/bảng trong datawarehouse");
                return false;
            }

        } catch (Exception e) {
            System.out.println("❌ Lỗi khi validate schema Aggregate: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // ==========================
    // VALIDATE CÁC BẢNG
    // ==========================

    /** FactWeather: SK, Day, WindKey, UVKey, Temperature, Humidity, DewPoint, Pressure, CloudCover, Visibility, CloudCeiling */
    private boolean validateFactWeather(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "SK",
                "DAY",
                "WINDKEY",
                "UVKEY",
                "TEMPERATURE",
                "HUMIDITY",
                "DEWPOINT",
                "PRESSURE",
                "CLOUDCOVER",
                "VISIBILITY",
                "CLOUDCEILING"
        );
        return validateColumns(conn, "FactWeather", requiredCols);
    }

    /** DimTime: TimeKey, FullDate, Day, Month, Year, Weekday */
    private boolean validateDimTime(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "TIMEKEY",
                "FULLDATE",
                "DAY",
                "MONTH",
                "YEAR",
                "WEEKDAY"
        );
        return validateColumns(conn, "DimTime", requiredCols);
    }

    /** date_dim: SK, DateOnly, Day, Month, Year, Weekday */
    private boolean validateDateDim(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "SK",
                "DATEONLY",
                "DAY",
                "MONTH",
                "YEAR",
                "WEEKDAY"
        );
        return validateColumns(conn, "date_dim", requiredCols);
    }

    /** aggregate_weather_daily: DateOnly, AvgTemp, MinTemp, MaxTemp, AvgHumidity, AvgPressure, RowCount */
    private boolean validateAggregateTable(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "DATEONLY",
                "AVGTEMP",
                "MINTEMP",
                "MAXTEMP",
                "AVGHUMIDITY",
                "AVGPRESSURE",
                "ROWCOUNT"
        );
        return validateColumns(conn, "aggregate_weather_daily", requiredCols);
    }


    // ==========================
    // HÀM GENERIC: kiểm tra cột
    // ==========================

    private boolean validateColumns(Connection conn,
                                    String tableName,
                                    List<String> requiredColumns) throws Exception {

        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, DB_NAME);
        ps.setString(2, tableName);
        ResultSet rs = ps.executeQuery();

        Set<String> existing = new HashSet<>();
        while (rs.next()) {
            existing.add(rs.getString("COLUMN_NAME").toUpperCase());
        }

        boolean ok = true;
        for (String col : requiredColumns) {
            if (!existing.contains(col.toUpperCase())) {
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