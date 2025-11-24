package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBConnection {

	// Kết nối database + kiểm tra hoạt động
	public static Connection connectDB(String host, int port, String user, String pass, String name) {
		try {
			// Chuỗi JDBC
			String url = "jdbc:mysql://" + host + ":" + port + "/" + name +
					"?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";

			Connection conn = DriverManager.getConnection(url, user, pass);

			System.out.println("🔗 Kết nối MySQL thành công: " + name);

			// TEST SQL
			ResultSet rs = conn.createStatement().executeQuery("SELECT NOW()");
			if (rs.next()) {
				System.out.println("⏱ DB Time = " + rs.getString(1));
			}
			return conn;

		} catch (SQLException e) {
			System.out.println("❌ Lỗi kết nối MySQL!");
			e.printStackTrace();
			return null;
		}
	}
}
