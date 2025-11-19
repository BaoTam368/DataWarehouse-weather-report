package database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataBase {

    //Phương thức kết nối database
    public static Connection connectDB(String host, int port, String user, String pass, String name) {
        try {
            // Chuỗi kết nối JDBC
            String url = "jdbc:mysql://" + host + ":" + port + "/" + name + "?useSSL=false&serverTimezone=UTC";
            Connection conn = DriverManager.getConnection(url, user, pass);
            System.out.println("Kết nối MySQL thành công!");
            return conn;
        } catch (SQLException e) {
            System.out.println("Kết nối thất bại!");
            return null;
        }
    }
}