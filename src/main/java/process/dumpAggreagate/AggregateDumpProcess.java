package process.dumpAggreagate;

import database.Control;
import database.DataBase;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Objects;

public class AggregateDumpProcess {

    /**
     * Dump bảng AggregateWeatherDaily -> file CSV
     *
     * @param sourceId  nguồn dữ liệu (config_source.source_id)
     * @param outputPath đường dẫn file CSV muốn xuất (ví dụ: "D:/DW/aggregate/aggregate_daily.csv")
     */
    public void dumpAggregateToCsv(int sourceId, String outputPath) {

        Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());
        boolean success = false;
        long sizeBytes = 0;

        Path out = Path.of(outputPath);

        // 1. Đọc từ warehouse và ghi file
        try (
                Connection warehouseConn = DataBase.connectDB("localhost", 3306, "root", "1234", "warehouse");
                PreparedStatement ps = Objects.requireNonNull(warehouseConn).prepareStatement(
                        "SELECT DateOnly, AvgTemp, MinTemp, MaxTemp, " +
                                "AvgHumidity, AvgPressure, RowCount " +
                                "FROM aggregate_weather_daily " +
                                "ORDER BY DateOnly"
                );
                BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)
        ) {
            writer.write("DateOnly,AvgTemp,MinTemp,MaxTemp,AvgHumidity,AvgPressure,RowCount");
            writer.newLine();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Date date = rs.getDate("DateOnly");
                double avgTemp = rs.getDouble("AvgTemp");
                double minTemp = rs.getDouble("MinTemp");
                double maxTemp = rs.getDouble("MaxTemp");
                double avgHumidity = rs.getDouble("AvgHumidity");
                double avgPressure = rs.getDouble("AvgPressure");
                int rc = rs.getInt("RowCount");

                String line = String.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d",
                        date.toString(),
                        avgTemp,
                        minTemp,
                        maxTemp,
                        avgHumidity,
                        avgPressure,
                        rc
                );
                writer.write(line);
                writer.newLine();
            }

            writer.flush();
            sizeBytes = Files.size(out);
            success = true;
            System.out.println("Dump aggregate -> CSV thành công! File: " + outputPath);

        } catch (Exception e) {
            System.out.println("Dump aggregate -> CSV thất bại!");
        }

        // 2. Ghi log (dù thành công hay thất bại vẫn cố log)
        Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());
        try (Connection controlConn = DataBase.connectDB("localhost", 3306, "root", "1234", "control")) {
            if (controlConn == null) {
                System.out.println("Không thể ghi log vì kết nối DB control thất bại");
                return;
            }

            Control.insertFileLog(
                    controlConn,
                    sourceId,
                    outputPath,
                    startTime,
                    (double) sizeBytes,       // size
                    success ? "SC" : "F",
                    endTime
            );

            Control.insertProcessLog(
                    controlConn,
                    sourceId,
                    "DP", // DUMP AGGREGATE
                    "Dump AggregateWeatherDaily to CSV",
                    success ? "SC" : "F",
                    startTime,
                    endTime
            );
        } catch (Exception e) {
            System.out.println("Ghi log cho dump aggregate thất bại!");
        }
    }

}