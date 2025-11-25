package process.mart;

import database.Control;
import email.EmailUtils;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class MartProcess {

    public void runMart(int sourceId, List<String> martSqlPath,
                        Connection martConn, Connection warehouseConn, Connection controlConn) {

        Timestamp validateStart = new Timestamp(System.currentTimeMillis());
        boolean success;

        try {
            if (martConn == null || warehouseConn == null || controlConn == null) {
                System.out.println("Kết nối mart/warehouse/control thất bại!");
                return;
            }

            // 1. VALIDATE SCHEMA -> MR
            boolean ready = isReady(sourceId, martConn, warehouseConn, controlConn, validateStart);
            if (!ready) {
                System.out.println("Schema không đúng, dừng load mart.");
                return;
            }

            // 2. LOAD MART -> LM
            martConn.setAutoCommit(false);
            Timestamp loadStart = new Timestamp(System.currentTimeMillis());

            success = isSuccess(martSqlPath, martConn, sourceId);

            Timestamp loadEnd = new Timestamp(System.currentTimeMillis());
            writeLoadLog(sourceId, controlConn, success, loadStart, loadEnd);

        } catch (Exception e) {
            System.out.println("Lỗi chung khi chạy MartProcess: " + e.getMessage());
            EmailUtils.send(
                    "Lỗi load dữ liệu sang mart",
                    "Source ID: " + sourceId + "\nChi tiết: " + e.getMessage()
            );
        } finally {
            closeQuietly(martConn);
            closeQuietly(warehouseConn);
            closeQuietly(controlConn);
        }
    }

    /**
     * Ghi log cho bước load mart (LM).
     *
     * @param sourceId    Nguồn dữ liệu (config_source.source_id)
     * @param controlConn Kết nối DB control
     * @param success     Trạng thái của quá trình load mart
     * @param loadStart   Thời điểm bắt đầu load mart
     * @param loadEnd     Thời điểm kết thúc load mart
     */
    private static void writeLoadLog(int sourceId, Connection controlConn,
                                     boolean success, Timestamp loadStart, Timestamp loadEnd) {

        Control.insertProcessLog(
                controlConn,
                sourceId,
                "LM",
                "Load dữ liệu vào mart_weather",
                success ? "SC" : "F",
                loadStart,
                loadEnd
        );
    }

    /**
     * Thực hiện danh sách script .sql để load dữ liệu vào martWeather
     *
     * @param martSqlPath danh sách các file .sql cần chạy trong quá trình load mart
     * @param martConn    kết nối DB mart
     * @return true nếu load mart thành công, false ngược lại
     * @throws SQLException nếu có lỗi khi thực thi các file .sql
     */
    private boolean isSuccess(List<String> martSqlPath, Connection martConn, int sourceId) throws SQLException {
        try {
            for (String path : martSqlPath) {
                executeSqlScript(martConn, path);
            }
            martConn.commit();
            System.out.println("Load mart thành công!");
            return true;
        } catch (Exception ex) {
            martConn.rollback();
            System.out.println("Load mart thất bại! Chi tiết: " + ex.getMessage());

            EmailUtils.send(
                    "Lỗi load dữ liệu sang mart",
                    "Source ID: " + sourceId + "\nChi tiết: " + ex.getMessage()
            );
            ex.printStackTrace();
            return false;
        }
    }

    /**
     * Check if mart is ready to load data.
     *
     * @param sourceId      Nguồn dữ liệu (config_source.source_id)
     * @param martConn      kết nối DB mart
     * @param warehouseConn kết nối DB datawarehouse
     * @param controlConn   kết nối DB control
     * @param validateStart Thời điểm bắt đầu validate
     * @return true nếu mart đã sẵn sàng, false ngược lại
     */
    private static boolean isReady(int sourceId, Connection martConn, Connection warehouseConn,
                                   Connection controlConn, Timestamp validateStart) {

        MartValidator validator = new MartValidator();
        boolean ready = validator.validateAll(martConn, warehouseConn);

        Timestamp validateEnd = new Timestamp(System.currentTimeMillis());

        Control.insertProcessLog(
                controlConn,
                sourceId,
                "MR",
                "Validate schema before load mart",
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
        } catch (Exception ignored) {
        }
    }
}