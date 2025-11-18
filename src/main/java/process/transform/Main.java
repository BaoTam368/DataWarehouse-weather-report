package process.transform;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        Config config = xmlMapper.readValue(new File("config.xml"), Config.class);
        List<String> paths = config.transaction.scripts;

        // Gọi process transform để chạy danh sách script transform
        TransformProcess process = new TransformProcess();
        process.runTransform(paths);
    }
}