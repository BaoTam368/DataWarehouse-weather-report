package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class Control {

	public static void insertFileLog(Connection conn, int srcId, String srcFileLocation, Timestamp time, String format,
			int record, double size, String status, Timestamp executeTime) {
		try {
			String sql = "INSERT INTO file_log "
					+ "(source_id, file_path, time, file_format, count, size, status, execute_time) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, srcId); // Nguồn dữ liệu
			ps.setString(2, srcFileLocation); // Đường dẫn file
			ps.setTimestamp(3, time);
			ps.setString(4, format); // format cua file
			ps.setInt(5, record); // Số bản ghi
			ps.setDouble(6, size); // Kích thước file
			ps.setString(7, status); // Trạng thái
			ps.setTimestamp(8, executeTime); // Thời điểm kết thúc
			ps.executeUpdate();
			System.out.println("Đã ghi file_log quá trình vào control.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void insertProcessLog(Connection conn, int srcId, String processCode, String processName,
			String status, Timestamp startTime, Timestamp endTime) {
		try {
			String sql = "INSERT INTO process_log "
					+ "(source_id, process_code, process_name, started_at, status, updated_at) "
					+ "VALUES (?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, srcId); // Nguồn dữ liệu
			ps.setString(2, processCode); // process code
			ps.setString(3, processName); // process name
			ps.setTimestamp(4, startTime); // Thời điểm bắt đầu
			ps.setString(5, status); // Trạng thái
			ps.setTimestamp(6, endTime); // Thời điểm kết thúc
			ps.executeUpdate();
			System.out.println("✅ Đã ghi file_log quá trình vào control.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int insertConfigSource(Connection conn, String sourceName, String sourceUrl,
			String sourceFileLocation, String fileFormat, String scrapingScriptPath, String destinationStaging,
			String transformProcedure, String loadWarehouseProcedure, String aggregateProcedure, String aggregateTable,
			String aggregateFilePath) {
		try {
			String sql = "INSERT INTO config_source "
					+ "(source_name, source_url, source_file_location, file_format, scraping_script_path, "
					+ "destination_staging, transform_procedure, load_warehouse_procedure, "
					+ "aggregate_procedure, aggregate_table, aggregate_file_path) "
					+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, sourceName);
			ps.setString(2, sourceUrl);
			ps.setString(3, sourceFileLocation);
			ps.setString(4, fileFormat);
			ps.setString(5, scrapingScriptPath);
			ps.setString(6, destinationStaging);
			ps.setString(7, transformProcedure);
			ps.setString(8, loadWarehouseProcedure);
			ps.setString(9, aggregateProcedure);
			ps.setString(10, aggregateTable);
			ps.setString(11, aggregateFilePath);
			ps.executeUpdate();
			// Lấy source_id vừa tạo
			ResultSet rs = ps.getGeneratedKeys();
			if (rs.next()) {
				int sourceId = rs.getInt(1);
				System.out.println("✅ Đã ghi config_source vào control với source_id = " + sourceId);
				return sourceId;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return -1; // Nếu thất bại
	}

	public static void insertConfigMart(Connection conn, String username, String remoteHost, String password,
			String fileFormat, String aggregateFilePath, String loadMartScriptPath) {
		try {
			String sql = "INSERT INTO config_mart "
					+ "(username, remote_host, passwword, file_format, aggregate_file_path, load_mart_script_path) "
					+ "VALUES (?, ?, ?, ?, ?, ?)";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, username);
			ps.setString(2, remoteHost);
			ps.setString(3, password);
			ps.setString(4, fileFormat);
			ps.setString(5, aggregateFilePath);
			ps.setString(6, loadMartScriptPath);
			ps.executeUpdate();
			System.out.println("✅ Đã ghi config_mart vào control.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
