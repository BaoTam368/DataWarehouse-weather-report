package status;

public enum Process {

	ER("Extract Ready", "Sẵn sàng để extract"), EO("Extract Ongoing", "Đang thực hiện extract"),
	EF("Extract Fail", "Extract thất bại"),

	TR("Transform Ready", "Sẵn sàng để transform"), TO("Transform Ongoing", "Đang thực hiện transform"),
	TF("Transform Fail", "Transform thất bại"),

	LR("Load Ready", "Sẵn sàng để load vào warehouse"), LO("Load Ongoing", "Đang thực hiện load vào warehouse"),
	LF("Load Fail", "Load vào warehouse thất bại"),

	SC("Success", "Hoàn thành ETL trong staging"), F("Fail", "Thất bại ETL trong staging");

	public final String meaning;
	public final String description;

	Process(String meaning, String description) {
		this.meaning = meaning;
		this.description = description;
	}

}
