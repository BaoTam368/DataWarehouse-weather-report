package extract;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.*;
import database.*;
import email.EmailUtils;
import extract.*;

public class MainExtract {
	// Phương thức kiểm tra xem dữ liệu có sẵn sàng để thực hiện bước Extract hay
	// không
	public static boolean checkExtract(Config config, Document doc) throws Exception {
		// Kiểm tra các điều kiện cần thiết để extract dữ liệu:
		// 1. URL của nguồn dữ liệu có thể truy cập được
		// 2. Thư mục lưu trữ dữ liệu nguồn tồn tại và hợp lệ
		// 3. Các selector trong tài liệu HTML hợp lệ và tồn tại
		if (Scraper.checkURL(config.source.source_url) && Scraper.checkPath(config.source.source_folder_path)
				&& Scraper.checkAllSelectors(doc)) {
			return true; // Nếu tất cả điều kiện đều đúng, trả về true
		}
		
		// Nếu bất kỳ điều kiện nào không thỏa, trả về false
		return false;

	}

	public static void runExtract(String[] args, Config config, int source_id) throws Exception {
		Connection conn = DataBase.connectDB(config.database.host, config.database.port, config.database.user,
				config.database.password, "control");
		String processStatus = "SC";
		String fileStatus = "F";

		// Kiem tra extract
		Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());

		Document doc = Scraper.connectToWebsite(config.source.source_url);
		Timestamp checkTime = Timestamp.valueOf(LocalDateTime.now());

		if (!checkExtract(config, doc)) {
			processStatus = "F";
			Control.insertProcessLog(conn, source_id, "ER", config.extract.process_name, processStatus, startTime,
					checkTime);
			Control.insertProcessLog(conn, source_id, "EF", config.extract.process_name, processStatus, checkTime,
					checkTime);
			conn.close();
			return;
		}
		Control.insertProcessLog(conn, source_id, "ER", config.extract.process_name, processStatus, startTime,
				checkTime);

		// Thuc hien extract
		Timestamp extractStart = Timestamp.valueOf(LocalDateTime.now());
		double fileSize = 0;

		try {
			WeatherData data = Scraper.fetchWeatherData(doc);
			String fileName = Scraper.generateFileName(config.source.source_folder_path);
			fileSize = Scraper.writeToCSV(data, fileName);

			fileStatus = "SC";
			processStatus = "SC";
		} catch (Exception e) {
			processStatus = "F";
			e.printStackTrace();

			Timestamp failTime = Timestamp.valueOf(LocalDateTime.now());

			Control.insertProcessLog(conn, source_id, "EF", config.extract.process_name, processStatus, extractStart,
					failTime);
			conn.close();
			return;
		}
		Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());
		Control.insertProcessLog(conn, source_id, "EO", config.extract.process_name, processStatus, extractStart,
				endTime);

		Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, fileSize, fileStatus,
				endTime);
		conn.close();

	}

	public static void main(String[] args) throws Exception {
		// Kiểm tra tham số
		if (args.length < 2) {
			System.err.println("Usage: java -jar extract.jar <config-path> <source-id>");
			return;
		}

		// Lấy tham số
		String configPath = args[0];
		int sourceId;
		try {
			sourceId = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			System.err.println("source_id phải là số nguyên, không phải: " + args[1]);
			return;
		}

		// Đọc config
		Config config = MainConfig.readConfig(configPath);
		if (config == null) {
			System.err.println("Không thể đọc file config từ: " + configPath);
			return;
		}

		// Ví dụ dùng sourceId
		System.out.println("Running ETL với source: " + sourceId);

		runExtract(args, config, sourceId);

	}
}
