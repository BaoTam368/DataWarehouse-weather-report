package process.loadwh;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;
import database.DBConnection;

import java.io.File;
import java.sql.Connection;

public class Main {

    public static void main(String[] args) {

        try {
            if (args.length == 0) {
                System.out.println("❌ Thiếu đường dẫn config.xml");
                return;
            }

            String configPath = args[0];

            XmlMapper mapper = new XmlMapper();
            Config cfg = mapper.readValue(new File(configPath), Config.class);

            // Connect DB
            String host = cfg.database.host;
            int port = cfg.database.port;
            String user = cfg.database.user;
            String password = cfg.database.password;
            Connection controlConn = DBConnection.connectDB(host, port, user, password, "control");
            Connection warehouseConn = DBConnection.connectDB(host, port, user, password, "datawarehouse");
            Connection stagingConn = DBConnection.connectDB(host, port, user, password, "staging");

            // Run load warehouse
            LoadWarehouseProcess loader = new LoadWarehouseProcess(cfg);
            loader.runLoadWarehouse(controlConn, warehouseConn, stagingConn);

            // Close
            if (controlConn != null) controlConn.close();
            if (warehouseConn != null) warehouseConn.close();
            if (stagingConn != null) stagingConn.close();

        } catch (Exception e) {
            System.out.println("❌ Lỗi MainLoadWarehouse: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
