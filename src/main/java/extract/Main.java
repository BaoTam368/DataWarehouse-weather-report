package extract;

import java.io.File;

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

public class Main {
	public static void main(String[] args, Connection conn, Config config, int source_id) throws Exception {
		Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());

		Document doc = Jsoup.connect(config.source.source_url).get();
		String fileStatus = "FI";
		String processStatus = "F";

		// kiem tra san sang de extract
		if (Scraper.checkURL(config.source.source_url) && Scraper.checkPath(config.source.source_folder_path)
				&& Scraper.checkAllSelectors(doc)) {
			fileStatus = "FV";
			processStatus = "ER";
		}
		Timestamp updateAt = Timestamp.valueOf(LocalDateTime.now());

		Control.insertProcessLog(conn, source_id, config.extract.process_code, config.extract.process_name,
				processStatus, startTime, updateAt);

		Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, 0, fileStatus, updateAt);
		if (fileStatus == "FI") {
			return;
		}

		// Thuc hien extract
		updateAt = Timestamp.valueOf(LocalDateTime.now());
		double fileSize = 0;

		try {
			processStatus = "EO";
			WeatherData data = Scraper.fetchWeatherData(doc);
			String fileName = Scraper.generateFileName(config.source.source_folder_path);
			fileSize = Scraper.writeToCSV(data, fileName);
			fileStatus = "FS";
		} catch (Exception e) {
			processStatus = "EF";
			e.printStackTrace();
		}
		Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());
		Control.insertProcessLog(conn, source_id, config.extract.process_code, config.extract.process_name,
				processStatus, updateAt, endTime);
		Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, fileSize, fileStatus,
				endTime);
	}
}
