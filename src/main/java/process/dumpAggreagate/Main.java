package process.dumpAggreagate;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;
import database.DataBase;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

public class Main {
    public static void main(String[] args) throws IOException {
        XmlMapper xmlMapper = new XmlMapper();
        Config config = xmlMapper.readValue(new File("config.xml"), Config.class);
        String outputPath = "data/dump_aggregate/aggregate_daily.csv";

        // Kết nối database
        String host = config.database.host;
        int port = config.database.port;
        String user = config.database.user;
        String password = config.database.password;
        Connection warehouseConn = DataBase.connectDB(host, port, user, password, "datawarehouse");
        Connection controlConn = DataBase.connectDB(host, port, user, password, "control");

        AggregateDumpProcess dumpProcess = new AggregateDumpProcess();
        dumpProcess.dumpAggregateToCsv(config.source.source_id, outputPath, warehouseConn, controlConn);

        System.out.println("=== DONE DUMP AGGREGATE ===");
    }
}