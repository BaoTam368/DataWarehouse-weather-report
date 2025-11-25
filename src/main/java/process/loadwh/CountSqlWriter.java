package process.loadwh;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;

public class CountSqlWriter extends PrintWriter {

    private final StringWriter buffer = new StringWriter();

    // Giữ danh sách bảng và số lượng
    private final Map<String, Integer> tableCounts = new LinkedHashMap<>();

    public CountSqlWriter() {
        super(new StringWriter());
    }

    @Override
    public void write(String s, int off, int len) {
        buffer.write(s, off, len);

        String[] lines = buffer.toString().split("\n");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;

            // Format ScriptRunner:
            // table_name: factweather total: 54
            if (line.matches("^table_name:\\s*\\w+\\s+total:\\s*\\d+$")) {
                try {
                    String[] parts = line.split("\\s+");

                    String tableName = parts[1].trim();
                    int value = Integer.parseInt(parts[parts.length - 1].trim());

                    tableCounts.put(tableName, value);

                } catch (Exception ignored) {}
            }
        }
    }

    public Map<String, Integer> getTableCounts() {
        return tableCounts;
    }

    public int getTotal() {
        return tableCounts.values().stream().mapToInt(i -> i).sum();
    }

    // Format in bảng đẹp
    public String getFormattedSummary() {
        StringBuilder sb = new StringBuilder();

        sb.append("=====================================\n");
        sb.append(" 📊 SUMMARY: DIM & FACT RECORD COUNT \n");
        sb.append("=====================================\n");

        for (var e : tableCounts.entrySet()) {
            sb.append(String.format("%-15s = %d\n", e.getKey(), e.getValue()));
        }

        sb.append("-------------------------------------\n");
        sb.append(String.format("%-15s = %d\n", "TOTAL", getTotal()));
        sb.append("=====================================\n");

        return sb.toString();
    }
}
