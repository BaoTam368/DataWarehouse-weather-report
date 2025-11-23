package process.extract;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.*;
import org.jsoup.nodes.Document;

public class Scraper {
	private static final Map<String, String> REQUIRED_SELECTORS = new HashMap<>();
	static {
		REQUIRED_SELECTORS.put("dayDate", "div.subnav-pagination div");
		REQUIRED_SELECTORS.put("temperature", "div.display-temp");
		REQUIRED_SELECTORS.put("uvIndex", "div.detail-item:nth-of-type(3) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("wind", "div.detail-item:nth-of-type(4) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("humidity", "div.detail-item:nth-of-type(6) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("dewPoint", "div.detail-item:nth-of-type(7) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("pressure", "div.detail-item:nth-of-type(8) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("cloudCover", "div.detail-item:nth-of-type(9) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("visibility", "div.detail-item:nth-of-type(10) div:nth-of-type(2)");
		REQUIRED_SELECTORS.put("ceiling", "div.detail-item:nth-of-type(11) div:nth-of-type(2)");
	}

	public static boolean checkURL(String url) throws IOException {
		// Kiểm tra URL có đáp ứng hay không (timeout 5 giây)
		Connection.Response resp = Jsoup.connect(url).timeout(5000).ignoreHttpErrors(true).execute();

		if (resp.statusCode() != 200) {
			System.err.println("❌ URL không phản hồi hoặc sai: HTTP " + resp.statusCode());
			return false;
		}

		System.out.println("✅ url hợp lệ.");
		return true;
	}

	public static boolean checkPath(String folderPath) throws IOException {
		// Kiểm tra folder lưu file
		File folder = new File(folderPath);
		if (!folder.exists() && !folder.mkdirs()) {
			System.err.println("❌ Không thể tạo folder: " + folderPath);
			return false;
		}

		// Test quyền ghi file
		File testFile = new File(folderPath + "/test_write.tmp");
		try (FileWriter fw = new FileWriter(testFile)) {
			fw.write("test");
		}
		testFile.delete();

		System.out.println("✅ folderPath hợp lệ.");
		return true;

	}

	public static boolean checkAllSelectors(Document doc) {
		for (Map.Entry<String, String> entry : REQUIRED_SELECTORS.entrySet()) {
			String field = entry.getKey();
			String selector = entry.getValue();

			if (doc.select(selector).isEmpty()) {
				System.err.println("❌ Missing selector for field: " + field + " | Selector: " + selector);
				return false;
			}
		}

		System.out.println("✅ Tất cả selector đều hợp lệ.");
		return true;
	}

	public static WeatherData fetchWeatherData(Document doc) throws Exception {

		WeatherData data = new WeatherData();
		data.time = getCurrentTime();

		for (Map.Entry<String, String> entry : REQUIRED_SELECTORS.entrySet()) {
			String field = entry.getKey();
			String selector = entry.getValue();
			String value = doc.select(selector).text();

			switch (field) {
			case "dayDate":
				data.dayDate = value;
				break;
			case "temperature":
				data.temperature = value;
				break;
			case "uvIndex":
				data.uvIndex = value;
				break;
			case "wind":
				data.wind = value;
				break;
			case "humidity":
				data.humidity = value;
				break;
			case "dewPoint":
				data.dewPoint = value;
				break;
			case "pressure":
				data.pressure = value;
				break;
			case "cloudCover":
				data.cloudCover = value;
				break;
			case "visibility":
				data.visibility = value;
				break;
			case "ceiling":
				data.ceiling = value;
				break;
			}
		}
		return data;
	}

	public static String getCurrentTime() {
		return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}

	public static String generateFileName(String folderPath) {
		String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss"));
		// Create directory if it doesn't exist
		File folder = new File(folderPath);
		if (!folder.exists()) {
			folder.mkdirs();
		}

		// Use File.separator for cross-platform compatibility
		return folder.getAbsolutePath() + File.separator + "weather_log(" + timestamp + ").csv";
	}

	public static double writeToCSV(WeatherData data, String filePath) {
		double kilobytes = 0;
		try (FileWriter fw = new FileWriter(filePath); PrintWriter pw = new PrintWriter(fw)) {

			pw.println(
					"FullDate,WeekDay,Day,Temperature,UVValue,WindDirection,Humidity,DewPoint,Pressure,CloudCover,Visibility,CloudCeiling");

			pw.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s%n", data.time, data.dayDate, data.temperature, data.uvIndex,
					data.wind, data.humidity, data.dewPoint, data.pressure, data.cloudCover, data.visibility,
					data.ceiling);

		} catch (Exception e) {
			e.printStackTrace();
		}
		File file = new File(filePath);
		if (file.exists()) {
			long bytes = file.length();
			kilobytes = (bytes / 1024.0);

		}
		return kilobytes;
	}
}