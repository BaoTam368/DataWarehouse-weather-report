package process.load;

import database.DataBase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class Main {

    // Đường dẫn file CSV cần load
    private static final Path CSV_PATH = Paths.get("D:\\DataWareHouse\\Data\\weather_log.csv");
    // Câu lệnh INSERT vào bảng staging (bảng temp)
    private static final String INSERT_SQL =
            "INSERT INTO temp (FullDate, Weekday, Day, Temperature, UVValue, WindDirection, Humidity, DewPoint, Pressure, Cloud, Visibility, CloudCeiling) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    public static void main(String[] args) {
        int success = 0;  // đếm số dòng insert thành công
        int fail = 0;     // đếm số dòng lỗi
        try (
                // Đọc file CSV
                BufferedReader br = new BufferedReader(new FileReader(CSV_PATH.toFile()));
                // Kết nối tới database staging
                Connection conn = DataBase.connectDB("localhost",3306,"root","123456","staging");
                // Chuẩn bị statement để insert dữ liệu
                PreparedStatement stmt = conn.prepareStatement(INSERT_SQL);
        ) {
            // Tắt chế độ auto-commit → chuyển sang dùng transaction
            conn.setAutoCommit(false);
            // Đọc dòng đầu tiên của CSV → đây là header nên bỏ qua
            String line = br.readLine();
            // Bắt đầu đọc từng dòng dữ liệu
            while ((line = br.readLine()) != null) {
                // Tách dòng CSV theo dấu phẩy
                // Tham số "-1" giúp giữ lại cột rỗng, tránh mất giá trị
                String[] c = line.split(",", -1);
                // Kiểm tra số cột
                if (c.length < 12) {
                    System.out.println("Dòng lỗi (không đủ 12 cột): " + line);
                    fail++;
                    continue; // bỏ qua dòng lỗi
                }
                try {
                    // Truyền giá trị vào 12 dấu ?
                    // Dùng trim() để xóa khoảng trắng dư thừa
                    for (int i = 0; i < 12; i++) {
                        stmt.setString(i + 1, c[i].trim());
                    }
                    // Thêm vào batch insert (chưa insert ngay)
                    stmt.addBatch();
                    success++;
                } catch (Exception ex) {
                    // Lỗi dữ liệu (ví dụ: kiểu sai, giá trị null không hợp lệ)
                    System.out.println("Lỗi dữ liệu dòng: " + line);
                    fail++;
                }
            }
            // Thực thi toàn bộ batch insert 1 lần cho nhanh
            stmt.executeBatch();
            // Commit transaction → ghi dữ liệu chính thức vào DB
            conn.commit();
            // In kết quả
            System.out.println("=== KẾT QUẢ LOAD CSV ===");
            System.out.println("✔ Thành công: " + success);
            System.out.println("✘ Thất bại : " + fail);
        } catch (Exception e) {
            // Bắt lỗi bất ngờ (kết nối DB, lỗi file…)
            e.printStackTrace();
        }
    }
}
