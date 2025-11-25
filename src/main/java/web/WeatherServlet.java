package web;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.gson.Gson;
import config.Config;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class WeatherServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {

        System.out.println("👉 [WeatherServlet] doGet() called");

        // 1. Đọc config.xml từ classpath (src/main/resources/config.xml)
        Config config;
        try (InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("config.xml")) {

            if (is == null) {
                System.err.println("❌ [WeatherServlet] config.xml not found in classpath");
                resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                resp.setContentType("application/json; charset=UTF-8");
                resp.getWriter().write("{\"error\":\"config.xml not found in classpath\"}");
                return;
            }

            XmlMapper xmlMapper = new XmlMapper();
            config = xmlMapper.readValue(is, Config.class);
        } catch (Exception e) {
            e.printStackTrace();
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.setContentType("application/json; charset=UTF-8");
            resp.getWriter().write("{\"error\":\"Failed to load config.xml: " + e.getMessage() + "\"}");
            return;
        }

        // 2. Lấy thông tin DB từ config
        String host = config.database.host;
        int port = config.database.port;
        String user = config.database.user;
        String password = config.database.password;

        resp.setCharacterEncoding("UTF-8");
        resp.setContentType("application/json; charset=UTF-8");

        List<Map<String, Object>> list = new ArrayList<>();

        String sql = """
                SELECT DateOnly, AvgTemp, MinTemp, MaxTemp, TempCategory
                FROM weatherdailysummary
                ORDER BY DateOnly
                """;

        // 3. Tự connect MySQL tới mart_weather (KHÔNG dùng DataBase.connectDB)
        String url = String.format(
                "jdbc:mysql://%s:%d/mart_weather?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                host, port
        );
        System.out.println("👉 [WeatherServlet] JDBC URL = " + url);

        try {
            // Đảm bảo driver MySQL được load (thường không cần, nhưng cho chắc)
            Class.forName("com.mysql.cj.jdbc.Driver");

            try (Connection conn = DriverManager.getConnection(url, user, password);
                 PreparedStatement st = conn.prepareStatement(sql);
                 ResultSet rs = st.executeQuery()) {

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("date", rs.getString("DateOnly"));
                    row.put("avg", rs.getDouble("AvgTemp"));
                    row.put("min", rs.getDouble("MinTemp"));
                    row.put("max", rs.getDouble("MaxTemp"));
                    row.put("cat", rs.getString("TempCategory"));
                    list.add(row);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            resp.getWriter().write("{\"error\":\"" + e.getMessage() + "\"}");
            return;
        }

        // 4. Trả JSON cho frontend
        String json = new Gson().toJson(list);
        resp.getWriter().write(json);
    }
}