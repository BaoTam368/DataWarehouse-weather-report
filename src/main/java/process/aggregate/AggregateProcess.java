package process.aggregate;

import database.DBConnection;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.util.List;

public class AggregateProcess {

    public void runAggregate(List<String> aggregateSqlPath) {

        try (Connection conn = DBConnection.connectDB("localhost", 3306, "root", "1234", "warehouse")) {
            // Kết nối DB warehouse
            if (conn != null) {
                conn.setAutoCommit(false);

                try {
                    for (String path : aggregateSqlPath) {
                        executeSqlScript(conn, path);
                    }
                    conn.commit();
                    System.out.println("Aggregate weather daily thành công!");
                } catch (Exception ex) {
                    conn.rollback();
                    System.out.println("Aggregate weather daily thất bại!");
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