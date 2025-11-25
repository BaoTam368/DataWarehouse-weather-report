package process.loadwh;

import config.Config;
import database.Control;
import email.EmailUtils;
import org.apache.ibatis.jdbc.ScriptRunner;

import java.io.FileReader;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Timestamp;

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
                cfg.warehouse.script,
                cfg.countSql != null ? cfg.countSql.path : null
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
            int total = 0;
            if (cfg.countSql != null) {
                total = runCountSql(warehouseConn, cfg.countSql.path);

                System.out.println("📊 Tổng số DIM + FACT: " + total);
            }

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
//    private void runSqlFile_JDBC(Connection conn, String path) throws Exception {
//        String sql = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)));
//
//        try (java.sql.Statement stmt = conn.createStatement()) {
//            stmt.execute(sql);
//        }
//    }



    private void callProcedure(Connection conn, String procName) throws Exception {
        String sql = "{CALL " + procName + "()}";
        try (CallableStatement stmt = conn.prepareCall(sql)) {
            stmt.execute();
        }
    }

    private int runCountSql(Connection conn, String path) throws Exception {
        ScriptRunner r = new ScriptRunner(conn);

        CountSqlWriter writer = new CountSqlWriter();

        r.setLogWriter(writer);   // <-- giờ đã đúng kiểu PrintWriter
        r.setErrorLogWriter(writer);

        r.setSendFullScript(true);

        try (Reader rd = new FileReader(path)) {
            r.runScript(rd);
        }
        return writer.getTotal();
    }
}
