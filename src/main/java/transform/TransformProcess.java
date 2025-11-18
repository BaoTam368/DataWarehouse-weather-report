package transform;

import org.apache.ibatis.jdbc.ScriptRunner;
import util.DBConnection;
import util.ProcessLogDAO;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;

public class TransformProcess {

    private final ProcessLogDAO processLogDAO = new ProcessLogDAO();

    public void runTransform(int sourceId, String transactionSqlPath) {
        Connection conn = null;
        try {
            conn = DBConnection.getRootConnection();
            conn.setAutoCommit(false);

            long processId = processLogDAO.startTransformProcess(conn, sourceId);

            try {
                executeSqlScript(conn, transactionSqlPath);

                conn.commit();
                processLogDAO.finishTransformProcess(conn, processId, "SUCCESS", null);
                System.out.println("Transform thành công!");
            } catch (Exception ex) {
                conn.rollback();
                processLogDAO.finishTransformProcess(conn, processId, "FAILED", ex.getMessage());
                ex.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBConnection.closeQuietly(conn);
        }
    }

    /**
     * Dùng MyBatis ScriptRunner để chạy script .sql
     */
    private void executeSqlScript(Connection conn, String filePath) throws Exception {
        ScriptRunner runner = new ScriptRunner(conn);

        runner.setSendFullScript(false);
        runner.setLogWriter(null);          // tắt log chi tiết ra console
        runner.setErrorLogWriter(null);

        try (Reader reader = new FileReader(filePath)) {
            runner.runScript(reader);
        }
    }
}