package extract;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.Config;
import database.*;
import email.EmailUtils;
import extract.*;

public class MainExtract {
	// Phương thức kiểm tra xem dữ liệu có sẵn sàng để thực hiện bước Extract hay không
	public static boolean checkExtract(Config config, Document doc) throws Exception {
	    // Kiểm tra các điều kiện cần thiết để extract dữ liệu:
	    // 1. URL của nguồn dữ liệu có thể truy cập được
	    // 2. Thư mục lưu trữ dữ liệu nguồn tồn tại và hợp lệ
	    // 3. Các selector trong tài liệu HTML hợp lệ và tồn tại
	    if (Scraper.checkURL(config.source.source_url) 
	        && Scraper.checkPath(config.source.source_folder_path)
	        && Scraper.checkAllSelectors(doc)) {
	        return true; // Nếu tất cả điều kiện đều đúng, trả về true
	    }
	    
	    // Nếu bất kỳ điều kiện nào không thỏa, trả về false
	    return false;
	}


	public static void main(String[] args, Connection conn, Config config, int source_id) throws Exception {
		String processStatus = "PS";
		String fileStatus = "?";

		// Kiem tra extract
		Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());

		Document doc = Scraper.connectToWebsite(config.source.source_url);
		if (!checkExtract(config, doc)) {
			processStatus = "PF";
		}

		Timestamp updateAt = Timestamp.valueOf(LocalDateTime.now());

		Control.insertProcessLog(conn, source_id, "ER", config.extract.process_name, processStatus, startTime,
				updateAt);
		if (processStatus == "PF") {
			return;
		}

		// Thuc hien extract
		updateAt = Timestamp.valueOf(LocalDateTime.now());
		double fileSize = 0;

		try {
			WeatherData data = Scraper.fetchWeatherData(doc);
			String fileName = Scraper.generateFileName(config.source.source_folder_path);
			fileSize = Scraper.writeToCSV(data, fileName);
			fileStatus = "FS";
			processStatus = "PS";
		} catch (Exception e) {
			processStatus = "PF";
			fileStatus = "FF";
			e.printStackTrace();
		}
		Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());

		Control.insertProcessLog(conn, source_id, "EO", config.extract.process_name, processStatus, updateAt, endTime);
		Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, fileSize, fileStatus,
				endTime);
	}
}
