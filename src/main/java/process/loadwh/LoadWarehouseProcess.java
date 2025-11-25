package process.loadwh;

import config.Config;
import database.Control;
import email.EmailUtils;
import java.sql.*;

public class LoadWarehouseProcess {

    private final Config cfg;

    public LoadWarehouseProcess(Config cfg) {
        this.cfg = cfg;
    }

    public void runLoadWarehouse(Connection controlConn, Connection warehouseConn, Connection stagingConn) {

        int sourceId = cfg.source.source_id;
        Timestamp startTime = new Timestamp(System.currentTimeMillis());

        // 1. LOG LR
        Control.insertProcessLog(controlConn, sourceId,
                "LOAD_WH",
                "LoadToWarehouse",
                "LR",
                startTime,
                startTime);

        // 2. VALIDATE
        LoadWarehouseValidator validator = new LoadWarehouseValidator();
        boolean ready = validator.validateAll(
                warehouseConn,
                stagingConn,
                "proc_load_warehouse",
                cfg.warehouse.script
        );

        if (!ready) {
            System.out.println("❌ Load không đủ điều kiện.");
            Control.insertFileLog(
                    controlConn,
                    sourceId,
                    "LOAD_WH",
                    startTime,
                    0,
                    "F",
                    new Timestamp(System.currentTimeMillis()));
            return;
        }

        // 3. LOG LO
        Control.insertProcessLog(
                controlConn,
                sourceId,
                "LOAD_WH",
                "LoadToWarehouse",
                "LO",
                startTime, new Timestamp(System.currentTimeMillis()));

        try {
            warehouseConn.setAutoCommit(false);

            //CALL PROCEDURE LOAD DỮ LIỆU
            callProcedure(warehouseConn, cfg.warehouse.procName);


            //COUNT DIM/FACT
            int total = countTotalDimFact(warehouseConn);
            System.out.println("📊 Tổng số DIM + FACT: " + total);


            //COMMIT
            warehouseConn.commit();

            // SUCCESS
            Control.insertFileLog(
                    controlConn,
                    sourceId,
                    "LOAD_WH",
                    startTime,
                    total,
                    "SC",
                    new Timestamp(System.currentTimeMillis()));

            System.out.println("🎉 LoadToWarehouse hoàn tất!");

        } catch (Exception ex) {
            try { warehouseConn.rollback();
            } catch (Exception ignored) {
            }

            EmailUtils.send("❌ Lỗi LoadWarehouse", ex.getMessage());

            // FAILED
            Control.insertFileLog(
                    controlConn,
                    sourceId,
                    "LOAD_WH",
                    startTime,
                    0,
                    "F",
                    new Timestamp(System.currentTimeMillis()));

            ex.printStackTrace();
        }
    }


    private void callProcedure(Connection conn, String procName) throws Exception {
        String sql = "{CALL " + procName + "()}";
        try (CallableStatement stmt = conn.prepareCall(sql)) {
            stmt.execute();
        }
    }

    private int countTotalDimFact(Connection conn) throws Exception {
        String[] tables = {"DimTime", "DimWind", "DimUV", "FactWeather"};
        int total = 0;

        for (String tbl : tables) {
            String sql = "SELECT COUNT(*) AS total FROM " + tbl;

            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                if (rs.next()) {
                    int cnt = rs.getInt("total");
                    System.out.println("📌 " + tbl + " = " + cnt);
                    total += cnt;
                }
            }
        }
        return total;
    }

}
