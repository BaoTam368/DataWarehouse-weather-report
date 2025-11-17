package extract;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.jsoup.*;
import org.jsoup.nodes.Document;

public class Scraper {

	public static WeatherData fetchWeatherData(String url) throws Exception {
		Document doc = Jsoup.connect(url).get();

		WeatherData data = new WeatherData();
		data.time = getCurrentTime();
		data.dayDate = doc.select("div.subnav-pagination div").text();
		data.temperature = doc.select("div.display-temp").text();
		data.uvIndex = doc.select("div.detail-item:nth-of-type(3) div:nth-of-type(2)").text();
		data.wind = doc.select("div.detail-item:nth-of-type(4) div:nth-of-type(2)").text();
		data.humidity = doc.select("div.detail-item:nth-of-type(6) div:nth-of-type(2)").text();
		data.dewPoint = doc.select("div.detail-item:nth-of-type(7) div:nth-of-type(2)").text();
		data.pressure = doc.select("div.detail-item:nth-of-type(8) div:nth-of-type(2)").text();
		data.cloudCover = doc.select("div.detail-item:nth-of-type(9) div:nth-of-type(2)").text();
		data.visibility = doc.select("div.detail-item:nth-of-type(10) div:nth-of-type(2)").text();
		data.ceiling = doc.select("div.detail-item:nth-of-type(11) div:nth-of-type(2)").text();

		return data;
	}

	public static String getCurrentTime() {
		return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}

	public static String generateFileName(String folderPath) {
		String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss"));
		// Tạo thư mục nếu chưa tồn tại
		String projectRoot = System.getProperty("user.dir");
		File folder = new File(folderPath);
		if (!folder.exists())
			folder.mkdirs();

		return folderPath + "/weather_log(" + timestamp + ").csv";
	}

	public static void writeToCSV(WeatherData data, String filePath) {
		try (FileWriter fw = new FileWriter(filePath); PrintWriter pw = new PrintWriter(fw)) {

			pw.println(
					"FullDate,WeekDay,Day,Temperature,UVValue,WindDirection,Humidity,DewPoint,Pressure,Cloud,Visibility,CloudCeiling");

			pw.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s%n", data.time, data.dayDate, data.temperature, data.uvIndex,
					data.wind, data.humidity, data.dewPoint, data.pressure, data.cloudCover, data.visibility,
					data.ceiling);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}