package aggregate;

import database.DataBase;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;

public class AggregateProcess {

    public void runAggregate(String aggregateSqlPath) {

        try (Connection conn = DataBase.connectDB("localhost", 3306, "root", "1234", "warehouse")) {
            // Kết nối DB warehouse
            if (conn != null) {
                conn.setAutoCommit(false);

                try {
                    executeSqlScript(conn, aggregateSqlPath);
                    conn.commit();
                    System.out.println("Aggregate thành công!");
                } catch (Exception ex) {
                    conn.rollback();
                    ex.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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