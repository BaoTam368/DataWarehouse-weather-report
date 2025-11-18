package process.aggregate;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        Config config = xmlMapper.readValue(new File("config.xml"), Config.class);

        // Gọi process aggregate
        AggregateProcess process = new AggregateProcess();
        process.runAggregate(config.aggregate.scripts);
    }
}