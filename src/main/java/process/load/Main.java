package process.load;

import database.DataBase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.Set;

public class Main {

    private static final Path FOLDER_PATH = Paths.get("data");

    // File log chứa danh sách file đã load rồi
    private static final Path LOADED_LOG = Paths.get("loaded_files.txt");

    private static final String INSERT_SQL =
            "INSERT INTO temp (FullDate, Weekday, Day, Temperature, UVValue, WindDirection, Humidity, DewPoint, Pressure, Cloud, Visibility, CloudCeiling) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public static void main(String[] args) {

        int totalSuccess = 0;
        int totalFail = 0;

        try (
                Connection conn = DataBase.connectDB("localhost", 3306, "root", "1234", "staging");
                PreparedStatement stmt = conn.prepareStatement(INSERT_SQL);
        ) {
            conn.setAutoCommit(false);

            // ================================
            // 1) Đọc danh sách file đã load
            // ================================
            Set<String> loadedFiles = new HashSet<>();

            if (Files.exists(LOADED_LOG)) {
                loadedFiles.addAll(Files.readAllLines(LOADED_LOG));
            } else {
                Files.createFile(LOADED_LOG);
            }

            // Lấy danh sách CSV trong thư mục
            File folder = FOLDER_PATH.toFile();
            File[] listFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

            if (listFiles == null || listFiles.length == 0) {
                System.out.println("❌ Không tìm thấy file CSV nào trong thư mục!");
                return;
            }

            // ================================
            // 2) Lặp qua từng file CSV
            // ================================
            for (File file : listFiles) {

                String filename = file.getName();

                // Nếu file đã load rồi => bỏ qua
                if (loadedFiles.contains(filename)) {
                    System.out.println("⏭ Bỏ qua file (đã load trước đó): " + filename);
                    continue;
                }

                System.out.println("🔄 Đang load file: " + filename);

                try (BufferedReader br = new BufferedReader(new FileReader(file))) {

                    String line = br.readLine(); // bỏ header

                    while ((line = br.readLine()) != null) {

                        String[] c = line.split(",", -1);
                        if (c.length < 12) {
                            System.out.println("Dòng lỗi (không đủ 12 cột): " + line);
                            totalFail++;
                            continue;
                        }

                        try {
                            for (int i = 0; i < 12; i++) {
                                stmt.setString(i + 1, c[i].trim());
                            }
                            stmt.addBatch();
                            totalSuccess++;

                        } catch (Exception ex) {
                            System.out.println("Lỗi dữ liệu dòng: " + line);
                            totalFail++;
                        }
                    }
                }

                // Ghi tên file này vào log => đánh dấu đã load
                try (FileWriter fw = new FileWriter(LOADED_LOG.toFile(), true)) {
                    fw.write(filename + System.lineSeparator());
                }
            }

            // Thực thi batch
            stmt.executeBatch();
            conn.commit();

            // Kết quả cuối cùng
            System.out.println("=== KẾT QUẢ LOAD TẤT CẢ FILE CSV ===");
            System.out.println("✔ Thành công: " + totalSuccess);
            System.out.println("✘ Thất bại : " + totalFail);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}