package com.example.DataWarehouse;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.Config;
import java.io.File;
import java.sql.Connection;
import java.util.List;

import database.DBConnection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import process.extract.Scraper;
import process.extract.WeatherData;
import process.loadcsv.loadCsvToStaging;
import process.loadwh.LoadWarehouseProcess;
import process.transform.TransformProcess;



public class Test {

	private static Connection controlConn;

	public static void main(String[] args) throws Exception {
		XmlMapper mapper = new XmlMapper();
		Config cfg = mapper.readValue(new File("config.xml"), Config.class);

		// Connect DB
		String host = cfg.database.host;
		int port = cfg.database.port;
		String user = cfg.database.user;
		String password = cfg.database.password;
		Connection controlConn = DBConnection.connectDB(host, port, user, password, "control");
		Connection warehouseConn = DBConnection.connectDB(host, port, user, password, "datawarehouse");
		Connection stagingConn = DBConnection.connectDB(host, port, user, password, "staging");


		//EXTRACT
        String url = cfg.source.source_url;
        Document doc = Jsoup.connect(url).get();

        WeatherData data = Scraper.fetchWeatherData(doc);
		String fileName = Scraper.generateFileName(cfg.source.source_folder_path);

		Scraper.writeToCSV(data, fileName);
		//LOAD CSV
		loadCsvToStaging.load("D:/DW/Datawarehouse/data/weather_log (1) (1).csv");

		//TRANSFORM
		int sourceId = cfg.source.source_id;
		List<String> paths = cfg.transaction.scripts;

		TransformProcess process = new TransformProcess();
		process.runTransform(sourceId, paths);

		// LOAD WAREHOUSE
		LoadWarehouseProcess loader = new LoadWarehouseProcess(cfg);
		loader.runLoadWarehouse(controlConn, warehouseConn, stagingConn);

		System.out.println("🎉 ETL COMPLETED SUCCESSFULLY!");
	}

}