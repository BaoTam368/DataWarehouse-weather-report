package config;

import java.io.File;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import email.EmailUtils;

public class MainConfig {
    public static Config readConfig() {
        try {
            // Tạo một đối tượng XmlMapper để đọc và ánh xạ dữ liệu XML sang đối tượng Java
            XmlMapper xmlMapper = new XmlMapper();

            // Đọc file "config.xml" và chuyển đổi nội dung của nó thành đối tượng Config
            return xmlMapper.readValue(new File("config.xml"), Config.class);
        } catch (Exception e) {
            // Nếu có lỗi xảy ra (ví dụ: file không tồn tại, định dạng XML sai)
            // Gửi email thông báo lỗi với tiêu đề và nội dung chi tiết lỗi
            EmailUtils.send("Lỗi hệ thống: không thể đọc file config.xml", "Chi tiết lỗi: " + e.getMessage());
        }

        // Nếu không thể đọc file hoặc xảy ra lỗi, trả về null
        return null;
    }

}