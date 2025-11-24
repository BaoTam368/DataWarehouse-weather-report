package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import email.EmailUtils;

public class DataBase {
	
	// Phương thức kết nối tới database MySQL
	public static Connection connectDB(String host, int port, String user, String pass, String name) {
	    try {
	        // Tạo chuỗi kết nối JDBC với thông tin host, port, tên database và các tham số kết nối
	        String url = "jdbc:mysql://" + host + ":" + port + "/" + name + "?useSSL=false&serverTimezone=UTC";
	        
	        // Tạo kết nối tới database bằng DriverManager
	        Connection conn = DriverManager.getConnection(url, user, pass);
	        
	        // Thông báo kết nối thành công ra console
	        System.out.println("Kết nối MySQL thành công!");
	        
	        // Trả về đối tượng Connection để thực hiện các thao tác với database
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