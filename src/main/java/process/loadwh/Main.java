package process.loadwh;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        LoadWarehouseProcess loadWH = new LoadWarehouseProcess();
        loadWH.runLoadWarehouse(List.of(
                "D:\\DW\\DataWarehouse\\database\\warehouse\\proc_load_warehouse.sql"
        ));

    }
}

