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

        // Gọi process transform để chạy danh sách script transform
        int sourceId = config.source.source_id;
        List<String> paths = config.transaction.scripts;

        TransformProcess process = new TransformProcess();
        process.runTransform(sourceId, paths);
    }
}