package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

    private static String HOST = "localhost";
    private static String PORT = "3306";
    private static String USER = "root";
    private static String PASS = "1234";

    // Load config từ XML nếu cần
    public static void loadConfig(String host, String port, String user, String pass) {
        HOST = host;
        PORT = port;
        USER = user;
        PASS = pass;
    }

    public static Connection getRootConnection() throws SQLException {
        String url = String.format(
                "jdbc:mysql://%s:%s/?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true",
                HOST, PORT
        );
        return DriverManager.getConnection(url, USER, PASS);
    }

    //  Đóng connection không bị crash
    public static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignored) {
            }
        }
    }
}