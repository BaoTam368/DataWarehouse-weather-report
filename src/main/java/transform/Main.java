package transform;

import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        // Giả sử source_id = 1 trong control.config_source
        int sourceId = 1;

        // Lấy thư mục gốc của project (nơi em chạy `java ...` hoặc `mvn exec:java`)
        String projectRoot = System.getProperty("user.dir");

        // Ghép path tương đối đến file transaction.sql
        String transactionSqlPath = Paths.get(projectRoot, "sql", "transaction.sql").toString();

        System.out.println("Project root: " + projectRoot);
        System.out.println("Transaction script: " + transactionSqlPath);

        // Gọi process transform
        TransformProcess process = new TransformProcess();
        process.runTransform(sourceId, transactionSqlPath);
    }
}