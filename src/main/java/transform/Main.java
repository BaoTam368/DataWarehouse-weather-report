package transform;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        Config config = xmlMapper.readValue(new File("config.xml"), Config.class);
        String transactionSqlPath = "database/staging/transaction.sql";

        String projectRoot = System.getProperty("user.dir");

        System.out.println("Project root: " + projectRoot);
        System.out.println("Transaction script: " + transactionSqlPath);

        // Gọi process transform
        TransformProcess process = new TransformProcess();
        process.runTransform(config.transaction.source_folder_path);
    }
}