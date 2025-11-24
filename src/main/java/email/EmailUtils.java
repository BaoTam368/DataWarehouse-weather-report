package email;

import jakarta.mail.*;
import jakarta.mail.internet.*;
import java.util.Properties;

public class EmailUtils {
	private static final String ADMIN_EMAIL = "23130286@st.hcmuaf.edu.vn"; 
	private static final String USERNAME = "baotam.testing.system@gmail.com"; // email gửi 
	private static final String PASSWORD = "epzm sdfa jhkh yobh"; // mật khẩu hoặc app password
	
	// Hàm gửi mail
	public static void send( String subject, String body) {
		try {
			Properties props = new Properties();
			props.put("mail.smtp.host", "smtp.gmail.com");
			props.put("mail.smtp.port", "587");
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.starttls.enable", "true");

			Session session = Session.getInstance(props, new Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(USERNAME, PASSWORD);
				}
			});

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(USERNAME));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(ADMIN_EMAIL));
			message.setSubject(subject);
			message.setText(body);

			Transport.send(message);
			System.out.println("Đã gửi mail");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		EmailUtils.send("Test", "Xin chào, hệ thống đã gửi mail thành công!");
	}
}
