package process.dumpAggreagate;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        Config config = xmlMapper.readValue(new File("config.xml"), Config.class);
        String outputPath = "data/dump_aggregate/aggregate_daily.csv";

        AggregateDumpProcess dumpProcess = new AggregateDumpProcess();
        dumpProcess.dumpAggregateToCsv(config.source.source_id, outputPath);

        System.out.println("=== DONE DUMP AGGREGATE ===");
    }
}