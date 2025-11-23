package extract;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.jsoup.*;
import org.jsoup.nodes.Document;

import email.EmailUtils;

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

	// Phương thức kết nối tới một website và lấy nội dung HTML
	public static Document connectToWebsite(String url) {
		try {
			// Sử dụng Jsoup để kết nối tới URL và lấy toàn bộ nội dung HTML
			return Jsoup.connect(url).get();
		} catch (Exception e) {
			// Nếu kết nối thất bại, gửi email thông báo lỗi với chi tiết
			EmailUtils.send("Lỗi hệ thống: không thể kết nối tới website: " + url, "Chi tiết lỗi: " + e.getMessage());
		}

		// Trả về null nếu không thể kết nối
		return null;
	}

	// Phương thức kiểm tra URL có thể truy cập được hay không
	public static boolean checkURL(String url) throws IOException {
		// Kết nối tới URL sử dụng Jsoup với timeout 5 giây
		// ignoreHttpErrors(true) để không ném lỗi nếu HTTP code khác 200
		Connection.Response resp = Jsoup.connect(url).timeout(5000).ignoreHttpErrors(true).execute();

		// Nếu HTTP status code khác 200, in lỗi và trả về false
		if (resp.statusCode() != 200) {
			System.err.println("❌ URL không phản hồi hoặc sai: HTTP " + resp.statusCode());
			return false;
		}

		// Nếu URL hợp lệ và phản hồi HTTP 200, in thông báo hợp lệ và trả về true
		System.out.println("✅ url hợp lệ.");
		return true;
	}

	// Phương thức kiểm tra folder lưu file có tồn tại và có quyền ghi hay không
	public static boolean checkPath(String folderPath) throws IOException {
		// Tạo đối tượng File đại diện cho folder
		File folder = new File(folderPath);

		// Nếu folder không tồn tại, thử tạo folder mới
		// Nếu không thể tạo được folder, in lỗi và trả về false
		if (!folder.exists() && !folder.mkdirs()) {
			System.err.println("❌ Không thể tạo folder: " + folderPath);
			return false;
		}

		// Kiểm tra quyền ghi file bằng cách tạo 1 file tạm trong folder
		File testFile = new File(folderPath + "/test_write.tmp");
		try (FileWriter fw = new FileWriter(testFile)) {
			fw.write("test"); // Ghi 1 nội dung nhỏ vào file
		}
		// Xóa file tạm sau khi test
		testFile.delete();

		// Nếu mọi thứ thành công, in thông báo hợp lệ và trả về true
		System.out.println("✅ folderPath hợp lệ.");
		return true;
	}

	// Phương thức kiểm tra tất cả các selector cần thiết có tồn tại trong tài liệu
	// HTML hay không
	public static boolean checkAllSelectors(Document doc) {
		// Duyệt qua từng cặp key-value trong REQUIRED_SELECTORS
		// key = tên trường, value = CSS selector
		for (Map.Entry<String, String> entry : REQUIRED_SELECTORS.entrySet()) {
			String field = entry.getKey(); // Tên trường dữ liệu
			String selector = entry.getValue(); // CSS selector tương ứng

			// Nếu selector không tìm thấy phần tử trong Document, in lỗi và trả về false
			if (doc.select(selector).isEmpty()) {
				System.err.println("❌ Missing selector for field: " + field + " | Selector: " + selector);
				return false;
			}
		}

		// Nếu tất cả selector đều tìm thấy, in thông báo hợp lệ và trả về true
		System.out.println("✅ Tất cả selector đều hợp lệ.");
		return true;
	}

	// Phương thức lấy dữ liệu thời tiết từ Document HTML
	public static WeatherData fetchWeatherData(Document doc) throws Exception {

		// Tạo đối tượng WeatherData mới để lưu thông tin thời tiết
		WeatherData data = new WeatherData();

		// Gán thời gian hiện tại vào dữ liệu
		data.time = getCurrentTime();

		// Duyệt qua tất cả các selector cần thiết trong REQUIRED_SELECTORS
		for (Map.Entry<String, String> entry : REQUIRED_SELECTORS.entrySet()) {
			String field = entry.getKey(); // Tên trường dữ liệu
			String selector = entry.getValue(); // CSS selector tương ứng

			// Lấy giá trị text từ selector trong Document
			String value = doc.select(selector).text();

			// Gán giá trị vào đúng trường của đối tượng WeatherData dựa trên tên field
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

		// Trả về đối tượng WeatherData đã được điền đầy đủ dữ liệu
		return data;
	}

	public static String getCurrentTime() {
		return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
	}

	// Phương thức tạo tên file mới cho dữ liệu thời tiết
	public static String generateFileName(String folderPath) {
		// Lấy thời gian hiện tại và định dạng theo "dd-MM-yyyy_HH-mm-ss"
		String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss"));

		// Tạo đối tượng File đại diện cho folder lưu trữ
		File folder = new File(folderPath);

		// Nếu folder chưa tồn tại, tạo mới
		if (!folder.exists())
			folder.mkdirs();

		// Trả về đường dẫn đầy đủ của file, kèm timestamp để tránh trùng tên
		return folderPath + "/weather_log(" + timestamp + ").csv";
	}

	// Phương thức ghi dữ liệu WeatherData vào file CSV
	// Trả về dung lượng file (KB) sau khi ghi
	public static double writeToCSV(WeatherData data, String filePath) {
		double kilobytes = 0;

		// Sử dụng FileWriter và PrintWriter để ghi dữ liệu vào file
		try (FileWriter fw = new FileWriter(filePath); PrintWriter pw = new PrintWriter(fw)) {

			// Ghi header của file CSV
			pw.println(
					"FullDate,WeekDay,Day,Temperature,UVValue,WindDirection,Humidity,DewPoint,Pressure,Cloud,Visibility,CloudCeiling");

			// Ghi dữ liệu thời tiết theo định dạng CSV
			pw.printf("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s%n", data.time, data.dayDate, data.temperature, data.uvIndex,
					data.wind, data.humidity, data.dewPoint, data.pressure, data.cloudCover, data.visibility,
					data.ceiling);

		} catch (Exception e) {
			// Nếu có lỗi, in stack trace ra console
			e.printStackTrace();
		}

		// Tính dung lượng file sau khi ghi
		File file = new File(filePath);
		if (file.exists()) {
			long bytes = file.length(); // Lấy dung lượng file tính bằng byte
			kilobytes = (bytes / 1024.0); // Chuyển đổi sang KB
		}

		// Trả về dung lượng file (KB)
		return kilobytes;
	}

}