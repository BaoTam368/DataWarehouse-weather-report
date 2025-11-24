package process.transform;

import database.DataBase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class TransformValidator {

    // Hàm gọi ngoài: validate toàn bộ schema cần cho transform
    public boolean validateAll(Connection connection) {
        try (connection) {
            if (connection == null) {
                System.out.println("❌ Không kết nối được DB staging để validate");
                return false;
            }

            boolean tempOk = validateTempTable(connection);
            boolean officialOk = validateOfficialTable(connection);

            if (tempOk && officialOk) {
                System.out.println("✅ Transform Ready (TR): Schema staging OK");
                return true;
            } else {
                System.out.println("❌ Transform NOT Ready: thiếu cột/bảng trong staging");
                return false;
            }

        } catch (Exception e) {
            System.out.println("❌ Lỗi khi validate schema");
            return false;
        }
    }

    // --- validate riêng từng bảng ---

    private boolean validateTempTable(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "FullDate", "Weekday", "Day",
                "Temperature", "UVValue", "Wind",
                "Humidity", "DewPoint", "Pressure",
                "Cloud", "Visibility", "CloudCeiling"
        );
        return validateColumns(conn, "temp", requiredCols);
    }

    private boolean validateOfficialTable(Connection conn) throws Exception {
        List<String> requiredCols = Arrays.asList(
                "FullDate", "Weekday", "Day",
                "Temperature", "UVValue", "WindDirection", "WindSpeed",
                "Humidity", "DewPoint", "Pressure", "Cloud",
                "Visibility", "CloudCeiling"
        );
        return validateColumns(conn, "official", requiredCols);
    }

    // --- hàm generic: kiểm tra 1 bảng có đủ các cột required hay không ---

    private boolean validateColumns(Connection conn, String tableName, List<String> requiredColumns) throws Exception {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, "staging");
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