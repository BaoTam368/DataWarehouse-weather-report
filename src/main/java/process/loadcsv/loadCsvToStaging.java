package process.loadcsv;

import database.DBConnection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class loadCsvToStaging {

    private static final int COLUMN_COUNT = 12;

    // ============================================================
    // HÀM CHÍNH: LOAD CSV → staging.stg_weather
    // ============================================================
                                                public static void load(String csvPath) {
        String sql = "INSERT INTO staging.stg_weather (" +
                "FullDate, Weekday, Day, Temperature, UVValue, Wind, Humidity, " +
                "DewPoint, Pressure, CloudCover, Visibility, CloudCeiling" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DBConnection.connectDB("localhost", 3306, "root", "123456", "staging");
             PreparedStatement ps = conn.prepareStatement(sql);
             BufferedReader br = new BufferedReader(new FileReader(csvPath))) {

            System.out.println("📥 Import CSV vào stg_weather...");
            br.readLine(); // Bỏ header

            int count = 0;
            String line;

            while ((line = br.readLine()) != null) {
                String[] arr = line.split(",");

                if (arr.length < COLUMN_COUNT) {
                    System.out.println("⚠ Bỏ qua dòng lỗi: " + line);
                    continue;
                }

                for (int i = 0; i < COLUMN_COUNT; i++) {
                    ps.setString(i + 1, arr[i].trim());
                }

                ps.addBatch();
                count++;
            }

            ps.executeBatch();
            System.out.println("✅ Đã import " + count + " dòng vào stg_weather");

        } catch (Exception e) {
            System.out.println("❌ Lỗi load CSV vào staging");
            e.printStackTrace();
        }
    }
}
