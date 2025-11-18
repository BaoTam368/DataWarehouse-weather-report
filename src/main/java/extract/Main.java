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
		String Filestatus = "FI";
		String ProcessStatus = "F";

		// kiem tra san sang de extract
		if (Scraper.checkURL(config.source.source_url) && Scraper.checkPath(config.source.source_folder_path)
				&& Scraper.checkAllSelectors(doc)) {
			Filestatus = "FV";
			ProcessStatus = "ER";
		}
		Timestamp updateAt = Timestamp.valueOf(LocalDateTime.now());

		Control.insertProcessLog(conn, source_id, config.extract.process_code, config.extract.process_name,
				ProcessStatus, startTime, updateAt);

		if (Filestatus == "FI") {
			Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, "csv", 0, 0, Filestatus,
					updateAt);
			return;
		}

		// Thuc hien extract
		updateAt = Timestamp.valueOf(LocalDateTime.now());
		double fileSize = 0;

		try {
			ProcessStatus = "EO";
			WeatherData data = Scraper.fetchWeatherData(doc);
			String fileName = Scraper.generateFileName(config.source.source_folder_path);
			Scraper.writeToCSV(data, fileName);
			Filestatus = "FS";
		} catch (Exception e) {
			ProcessStatus = "EF";
			e.printStackTrace();
		}
		Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());
		Control.insertProcessLog(conn, source_id, config.extract.process_code, config.extract.process_name,
				ProcessStatus, updateAt, endTime);
		Control.insertFileLog(conn, source_id, config.source.source_folder_path, startTime, "csv", 0, fileSize,
				Filestatus, endTime);
	}
}
