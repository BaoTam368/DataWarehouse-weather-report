package database;

import email.EmailUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnection {

	// Kết nối database + kiểm tra hoạt động
	public static Connection connectDB(String host, int port, String user, String pass, String name) {
		try {
			// Chuỗi JDBC
			String url = "jdbc:mysql://" + host + ":" + port + "/" + name + "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";

			// Tạo kết nối tới database bằng DriverManager
			Connection conn = DriverManager.getConnection(url, user, pass);

			// Thông báo kết nối thành công ra console
			System.out.println("🔗 Kết nối MySQL thành công: " + name);

			// TEST SQL
			ResultSet rs = conn.createStatement().executeQuery("SELECT NOW()");
			if (rs.next()) {
				System.out.println("⏱ DB Time = " + rs.getString(1));
			}
			return conn;

		} catch (SQLException e) {
			// Nếu kết nối thất bại, gửi email thông báo lỗi với chi tiết
			EmailUtils.send("Lỗi hệ thống: không thể kết nối database: " + name,
					"Chi tiết lỗi: " + e.getMessage());

			// Trả về null nếu không thể kết nối
			return null;
		}
	}
}
