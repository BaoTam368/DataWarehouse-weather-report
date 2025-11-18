package util;

import java.sql.*;
import java.time.LocalDateTime;

public class ProcessLogDAO {

    /**
     * Ghi 1 dòng process_log với status = RUNNING.
     * Trả về process_id để sau này cập nhật.
     */
    public long startTransformProcess(Connection conn, int sourceId) throws SQLException {
        String sql = "INSERT INTO control.process_log (source_id, process_code, process_name, started_at, status, " + "updated_at) VALUES (?, 'TRANSFORM', 'Transform temp to official', ?, 'RUNNING', ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            ps.setInt(1, sourceId);
            ps.setTimestamp(2, now);
            ps.setTimestamp(3, now);
            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Không lấy được process_id từ process_log");
    }

    /**
     * Cập nhật trạng thái SUCCESS / FAILED cho process_log
     */
    public void finishTransformProcess(Connection conn, long processId, String status, String errorMessage) throws SQLException {
        String sql =
                "UPDATE control.process_log SET status = ?, updated_at = ?, process_name = CONCAT( process_name, IF( ? IS NULL OR ? = '', '', CONCAT(' | Error: ', ?))) WHERE process_id = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            ps.setString(1, status);
            ps.setTimestamp(2, now);
            ps.setString(3, errorMessage);
            ps.setString(4, errorMessage);
            ps.setString(5, errorMessage);
            ps.setLong(6, processId);
            ps.executeUpdate();
        }
    }
}