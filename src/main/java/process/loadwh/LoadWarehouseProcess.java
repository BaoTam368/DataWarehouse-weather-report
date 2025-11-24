package process.loadwh;

import database.Control;
import database.DBConnection;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;


public class LoadWarehouseProcess {

    private static final String COUNT_SQL_PATH = "D:\\DW\\DataWarehouse\\database\\warehouse\\count_tables.sql";

    // hàm lấy source_id từ db.control
    public int getSourceIdByName(String sourceName) {
        int id = -1;

        try (Connection conn = DBConnection.connectDB(
                "localhost", 3306, "root", "123456", "control")) {

            if (conn == null) {
                System.out.println("❌ Không kết nối được DB control");
                return -1;
            }

            String sql = "SELECT source_id FROM config_source WHERE source_name = ? LIMIT 1";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, sourceName);
                var rs = ps.executeQuery();

                if (rs.next()) {
                    id = rs.getInt("source_id");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return id;
    }


    public void runLoadWarehouse(String sourceName) {

        // Lấy source_id từ database
        int source_id = getSourceIdByName(sourceName);
        if (source_id == -1) {
            System.out.println("Không tìm thấy source_id cho source: " + sourceName);
            return;
        }

        long startMillis = System.currentTimeMillis();
        Timestamp startTime = new Timestamp(startMillis);

        // 1.Kết nối tới DB control
        try (Connection controlConn = DBConnection.connectDB(
                "localhost", 3306, "root", "123456", "control")) {

            if (controlConn == null) {
                System.out.println("❌ KHÔNG kết nối được tới DB control!");
                return;
            }

            // ghi log Load Ready (LR)
            Control.insertProcessLog(
                    controlConn,
                    source_id,
                    "proc_load_warehouse",
                    "LoadToWarehouse",
                    "LR",
                    startTime,
                    startTime
            );

            // 2. Ket noi DB warehouse
            try (Connection WarehouseConn = DBConnection.connectDB(
                    "localhost", 3306, "root", "123456", "datawarehouse")) {

                if (WarehouseConn == null) {
                System.out.println("❌ Không thể kết nối DB datawarehouse!");

                Timestamp endTime = new Timestamp(System.currentTimeMillis());

                    Control.insertFileLog(
                            controlConn,
                            source_id,
                            "count_tables.sql",
                            startTime,
                            0,
                            "LF",
                            endTime
                    );
                return;
            }
                WarehouseConn.setAutoCommit(false);

            System.out.println("🔗 Kết nối thành công vào DB datawarehouse.");

            // goi log Load Ongoing (LO)
                Control.insertProcessLog(
                        controlConn,
                        source_id,
                        "proc_load_warehouse",
                        "LoadToWarehouse",
                        "LO",
                        startTime,
                        new Timestamp(System.currentTimeMillis())
                );

            // 3. Chạy PROC LOAD WAREHOUSE
            callProcedure(WarehouseConn, "proc_load_warehouse");
            WarehouseConn.commit();
            System.out.println("✔ Chạy proc_load_warehouse thành công.");

            // 4. Chạy count_tables.sql để lấy số lượng DIM + FACT
                String countSummary = runSqlFileForSelect(WarehouseConn,COUNT_SQL_PATH);

                System.out.println("📊 DIM + FACT SUMMARY:");
                System.out.println(countSummary);

                Timestamp endTime = new Timestamp(System.currentTimeMillis());

                //SUCCESS
                int totalSize = parseTotalCount(countSummary);
                Control.insertFileLog(
                        controlConn,
                        source_id,
                        "count_tables.sql",
                        startTime,
                        totalSize,
                        "SC",           // SUCCESS
                        endTime
                );

                System.out.println("✔ LoadToWarehouse — ghi log thành công!");

            } catch (Exception ex) {

                Timestamp endTime = new Timestamp(System.currentTimeMillis());

                //FAIL
                Control.insertFileLog(
                        controlConn,
                        source_id,
                        "count_tables.sql",
                        startTime,
                        0,
                        "LF",
                        endTime
                );

                System.out.println("❌ LoadToWarehouse thất bại!");
                ex.printStackTrace();
            }

        } catch (Exception ex) {
            System.out.println("❌ Lỗi kết nối tới control DB");
            ex.printStackTrace();
        }
    }

    //
    private int parseTotalCount(String countSummary) {
        if (countSummary == null || countSummary.isBlank()) return 0;

        int total = 0;

        // Tách theo dòng
        String[] lines = countSummary.split("\n");

        for (String line : lines) {
            line = line.trim();

            if (line.isEmpty()) continue;

            // Tìm dấu '='
            int idx = line.indexOf("=");

            if (idx != -1 && idx + 1 < line.length()) {
                try {
                    // Lấy phần sau dấu '=' và trim
                    int value = Integer.parseInt(line.substring(idx + 1).trim());
                    total += value;
                } catch (NumberFormatException e) {
                    System.out.println("⚠ Không parse được số từ dòng: " + line);
                }
            }
        }

        return total;
    }

    // 5. GỌI STORED PROCEDURE
    private void callProcedure(Connection WarehouseConn, String procName) throws Exception {
        String sql = "{CALL " + procName + "()}";
        try (CallableStatement stmt = WarehouseConn.prepareCall(sql)) {
            stmt.execute();
        }
    }

    // 6. CHẠY FILE SQL count_tables.sql COUNT DIM + FACT
    private String runSqlFileForSelect(Connection conn, String sqlFile) {
        StringBuilder log = new StringBuilder();

        try {
            String sql = new String(Files.readAllBytes(Paths.get(sqlFile)));
            String[] queries = sql.split(";");

            for (String q : queries) {
                q = q.trim();
                if (q.isEmpty()) continue;

                try (PreparedStatement ps = conn.prepareStatement(q)) {
                    var rs = ps.executeQuery();
                    while (rs.next()) {
                        String table = rs.getString("table_name");
                        int total = rs.getInt("total");

                        log.append(table)
                                .append(" = ")
                                .append(total)
                                .append("\n");
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }

        return log.toString();
    }


}
