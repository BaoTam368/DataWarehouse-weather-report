package status;

public enum File {
	FV("File Validated", "File đã được xác nhận hợp lệ"), FI("File Invalid", "File không hợp lệ"),
	FS("File Success", "File được đánh dấu đã xử lý xong"),
	FA("File Archived", "File được chuyển sang thư mục archive"), FF("File Fail", "File lỗi");

	public final String meaning;
	public final String description;

	File(String meaning, String description) {
		this.meaning = meaning;
		this.description = description;
	}
}
