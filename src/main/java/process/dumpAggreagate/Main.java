package process.dumpAggreagate;

public class Main {
    public static void main(String[] args) {

        int sourceId = 1; // phải khớp với control.config_source
        String outputPath = "data/dump_aggregate/aggregate_daily.csv";

        AggregateDumpProcess dumpProcess = new AggregateDumpProcess();
        dumpProcess.dumpAggregateToCsv(sourceId, outputPath);

        System.out.println("=== DONE DUMP AGGREGATE ===");
    }
}