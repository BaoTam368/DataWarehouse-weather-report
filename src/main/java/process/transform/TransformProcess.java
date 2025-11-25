package process.transform;

import database.Control;
import email.EmailUtils;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class TransformProcess {

    public void runTransform(int sourceId, List<String> transactionSqlPath,
                             Connection stagingConn, Connection controlConn) {

        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success;

        try {
            if (stagingConn == null || controlConn == null) {
                System.out.println("Kết nối DB staging/control thất bại");
                return;
            }

            // 1. VALIDATE -> TR
            boolean ready = isReady(sourceId, stagingConn, controlConn, validateStart);
            if (!ready) {
                System.out.println("Schema không đúng, dừng Transform.");
                return;
            }

            // 2. RUN TRANSFORM -> TO
            stagingConn.setAutoCommit(false);
            Timestamp transformStart = new Timestamp(System.currentTimeMillis());

            success = isSuccess(transactionSqlPath, stagingConn, sourceId);

            Timestamp transformEnd = new Timestamp(System.currentTimeMillis());
            writeTransformLog(sourceId, controlConn, success, transformStart, transformEnd);

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy Transform: " + e.getMessage());
            EmailUtils.send(
                    "Lỗi Transform dữ liệu staging",
                    "Source ID: " + sourceId + "\nChi tiết: " + e.getMessage()
            );
            e.printStackTrace();
        } finally {
            closeQuietly(stagingConn);
            closeQuietly(controlConn);
        }
    }

    private static void writeTransformLog(int sourceId, Connection controlConn,
                                          boolean success, Timestamp start, Timestamp end) {
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "TO",
                "Run transform scripts",
                success ? "SC" : "F",
                start,
                end
        );
    }

    private boolean isSuccess(List<String> transactionSqlPath, Connection stagingConn, int sourceId) throws SQLException {
        try {
            for (String path : transactionSqlPath) {
                executeSqlScript(stagingConn, path);
            }
            stagingConn.commit();
            System.out.println("Transform thành công!");
            return true;
        } catch (Exception ex) {
            stagingConn.rollback();
            System.out.println("Transform thất bại! Chi tiết: " + ex.getMessage());

            EmailUtils.send(
                    "Lỗi Transform dữ liệu staging",
                    "Source ID: " + sourceId + "\nChi tiết: " + ex.getMessage()
            );
            ex.printStackTrace();
            return false;
        }
    }

    private static boolean isReady(int sourceId, Connection stagingConn, Connection controlConn,
                                   Timestamp validateStart) {

        TransformValidator validator = new TransformValidator();
        boolean ready = validator.validateAll(stagingConn);

        Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

        Control.insertProcessLog(
                controlConn,
                sourceId,
                "TR",
                "Validate schema before transform",
                ready ? "SC" : "F",
                validateStart,
                validateEnd
        );

        return ready;
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

    private void closeQuietly(Connection conn) {
        try {
            if (conn != null) conn.close();
        } catch (Exception ignored) {}
    }
}