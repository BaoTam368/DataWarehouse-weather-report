package process.dumpAggreagate;

import database.Control;
import database.DataBase;
import email.EmailUtils;

import java.io.BufferedWriter;
import java.io.IOException;
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
     * @param sourceId   nguồn dữ liệu (config_source.source_id)
     * @param outputPath đường dẫn file CSV muốn xuất (ví dụ: "D:/DW/aggregate/aggregate_daily.csv")
     */
    public void dumpAggregateToCsv(int sourceId, String outputPath, Connection warehouseConn, Connection controlConn) {
        // Thời gian bắt đầu
        Timestamp startTime = Timestamp.valueOf(LocalDateTime.now());
        boolean success = false;
        long sizeBytes = 0;

        Path out = Path.of(outputPath);

        // 1. Đọc từ warehouse và ghi file
        try (
                warehouseConn;
                PreparedStatement ps = Objects.requireNonNull(warehouseConn).prepareStatement(
                        "SELECT DateOnly, AvgTemp, MinTemp, MaxTemp, " +
                                "AvgHumidity, AvgPressure, RowCount " +
                                "FROM aggregate_weather_daily " +
                                "ORDER BY DateOnly"
                );
                BufferedWriter writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)
        ) {
            // Ghi header
            writer.write("DateOnly,AvgTemp,MinTemp,MaxTemp,AvgHumidity,AvgPressure,RowCount");
            writer.newLine();

            // Ghi dữ liệu
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                // Ghi từng dòng
                writeLine(rs, writer);
            }

            // Ghi file và tính kích thước , flush để đảm bảo ghi xong
            writer.flush();
            sizeBytes = Files.size(out);
            success = true;
            System.out.println("Dump aggregate -> CSV thành công! File: " + outputPath);

        } catch (Exception e) {
            System.out.println("Dump aggregate -> CSV thất bại!");
            System.out.println("Chi tiết lỗi khi dump file: " + e.getMessage());
        }

        // 2. Ghi log
        Timestamp endTime = Timestamp.valueOf(LocalDateTime.now());
        writeLog(controlConn, sourceId, outputPath, startTime, (double) sizeBytes, success, endTime);
    }

    /**
     * Write log for dump aggregate process
     *
     * @param sourceId   Nguồn dữ liệu (config_source.source_id)
     * @param outputPath Đường dẫn file đã được dump
     * @param startTime  Thời điểm bắt đầu dump
     * @param sizeBytes  Kích thước file đã được dump (byte)
     * @param success    Trạng thái của quá trình dump
     * @param endTime    Thời điểm kết thúc dump
     */
    private static void writeLog(Connection controlConn, int sourceId, String outputPath, Timestamp startTime,
                                 double sizeBytes,
                                 boolean success, Timestamp endTime) {
        try (controlConn) {
            if (controlConn == null) {
                System.out.println("Không thể ghi log vì kết nối DB control thất bại");
                return;
            }

            Control.insertFileLog(
                    controlConn,
                    sourceId,
                    outputPath,
                    startTime,
                    sizeBytes,       // size
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
            EmailUtils.send(
                    "Lỗi khi dump aggregate file",
                    "Source ID: " + sourceId + "\nChi tiết: " + e.getMessage()
            );
        }
    }

    /**
     * Writes a line of the aggregated weather data to the given writer.
     * The line format is:
     * DateOnly,AvgTemp,MinTemp,MaxTemp,AvgHumidity,AvgPressure,RowCount
     *
     * @param rs     the result set containing the aggregated weather data
     * @param writer the writer to write the line to
     * @throws SQLException if the result set does not contain the expected columns
     * @throws IOException  if the writer throws an IOException
     */
    private static void writeLine(ResultSet rs, BufferedWriter writer) throws SQLException, IOException {
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

}