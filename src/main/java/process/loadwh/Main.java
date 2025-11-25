package process.loadwh;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import config.Config;
import database.DBConnection;

import java.io.File;
import java.sql.Connection;

public class Main {

    public static void main(String[] args) {

        try {
            // Load config.xml (để lấy script)
            XmlMapper mapper = new XmlMapper();
            Config cfg = mapper.readValue(new File("config.xml"), Config.class);

            // Connect DB
            Connection controlConn = DBConnection.connectDB("localhost", 3306, "root", "123456", "control");
            Connection warehouseConn = DBConnection.connectDB("localhost", 3306, "root", "123456", "datawarehouse");
            Connection stagingConn = DBConnection.connectDB("localhost", 3306, "root", "123456", "staging");

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