package com.example.DataWarehouse;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.*;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.Config;
import database.Control;
import database.DataBase;

import java.io.File;
import java.sql.Connection;

import extract.*;

public class Test {

	public static void main(String[] args) throws Exception {
		XmlMapper xmlMapper = new XmlMapper();
		Config config = xmlMapper.readValue(new File("config.xml"), Config.class);

		Connection conn = DataBase.connectDB(config.database.host, config.database.port, config.database.user,
				config.database.password, "control");

		int source_id = Control.insertConfigSource(conn, config.source.source_name, config.source.source_url,
				config.source.source_folder_path, "csv", config.extract.scraping_script_path, "?", "?", "?", "?", "?", "?");

		Main.main(args, conn, config, source_id);

	}

}
