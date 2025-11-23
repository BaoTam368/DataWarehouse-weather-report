package process.mart;

import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;

public class MartProcess {
    public void runMart(List<String> martSqlPath) {
        try (Connection conn = DataBase.connectDB("localhost", 3306, "root", "1234", "mart_weather")) {
            // Kết nối DB mart_weather
            if (conn != null) {
                conn.setAutoCommit(false);

                try {
                    for (String path : martSqlPath) {
                        executeSqlScript(conn, path);
                    }
                    conn.commit();
                    System.out.println("load mart thành công!");
                } catch (Exception ex) {
                    conn.rollback();
                    System.out.println("load mart thất bại!");
                }
            }
        } catch (Exception e) {
            System.out.println("Kết nối thất bại!");
        }
    }

    private void executeSqlScript(Connection conn, String filePath) throws Exception {
        ScriptRunner runner = new ScriptRunner(conn);
        runner.setSendFullScript(false);
        runner.setLogWriter(null);
        runner.setErrorLogWriter(null);

        try (Reader reader = new FileReader(filePath)) {
            runner.runScript(reader);
        }
    }
}