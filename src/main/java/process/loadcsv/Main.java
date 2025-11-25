package process.loadcsv;

public class Main {

    public static void main(String[] args) {
        System.out.println("===== START LOAD CSV → STAGING =====");
        loadCsvToStaging.load("D:/DW/Datawarehouse/data/weather_log (1) (1).csv");
    }
}
