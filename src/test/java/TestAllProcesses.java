import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;

import java.io.File;

public class TestAllProcesses {

    public static void main(String[] args) {

        try {
            System.out.println("========== BẮT ĐẦU TEST TOÀN BỘ ETL ==========\n");

            // ====== Load config ======
            XmlMapper mapper = new XmlMapper();
            Config config = mapper.readValue(new File("config.xml"), Config.class);

            String host = config.database.host;
            int port = config.database.port;
            String user = config.database.user;
            String password = config.database.password;

            int sourceId = config.source.source_id;

            // =====================================================================
            // 1️⃣ LOAD CSV → STAGING.TEMP
            // =====================================================================
            System.out.println("\n===== 1. LOAD CSV → staging.temp =====");
            process.load.Main.main(null);

            // =====================================================================
            // 2️⃣ TRANSFORM → STAGING.OFFICIAL
            // =====================================================================
            System.out.println("\n===== 2. TRANSFORM → staging.official =====");
            process.transform.Main.main(null);

            // =====================================================================
            // 3️⃣ LOAD WAREHOUSE → DIM + FACT
            // =====================================================================
            System.out.println("\n===== 3. LOAD WAREHOUSE → Dim / Fact =====");
            process.loadwh.Main.main(null);

            // =====================================================================
            // 4️⃣ AGGREGATE → aggregate_weather_daily
            // =====================================================================
            System.out.println("\n===== 4. AGGREGATE → aggregate_weather_daily =====");
            process.aggregate.Main.main(null);

            // =====================================================================
            // 5️⃣ DUMP AGGREGATE → CSV FILE
            // =====================================================================
            System.out.println("\n===== 5. DUMP AGGREGATE → CSV =====");
            process.dumpAggregate.Main.main(null);

            // =====================================================================
            // 6️⃣ LOAD MART → WeatherDailySummary
            // =====================================================================
            System.out.println("\n===== 6. LOAD MART → WeatherDailySummary =====");
            process.mart.Main.main(null);

            System.out.println("\n========== ETL HOÀN TẤT THÀNH CÔNG ==========");

        } catch (Exception e) {
            System.out.println("\n❌ LỖI TRONG QUÁ TRÌNH TEST ETL:");
            e.printStackTrace();
        }
    }
}