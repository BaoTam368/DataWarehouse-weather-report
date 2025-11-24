package com.example.DataWarehouse;

import config.Config;
import config.MainConfig;
import database.Control;
import database.DataBase;

import java.sql.Connection;

import process.extract.*;

public class Test {

    public static void main(String[] args) throws Exception {

        Config config = MainConfig.readConfig();

        Connection conn = DataBase.connectDB(config.database.host, config.database.port, config.database.user,
                config.database.password, "control");

        int source_id = Control.insertConfigSource(conn, config.source.source_name, config.source.source_url,
                config.source.source_folder_path, "csv", config.extract.scraping_script_path, "?", "?", "?", "?", "?",
                "?");

        MainExtract.main(args, conn, config, source_id);

    }

}