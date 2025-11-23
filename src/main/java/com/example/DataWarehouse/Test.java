package com.example.DataWarehouse;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.Config;
import java.io.File;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import process.extract.Scraper;
import process.extract.WeatherData;
import process.loadcsv.loadCsvToStaging;
import process.loadwh.LoadWarehouseProcess;
import process.transform.TransformProcess;


public class Test {

	public static void main(String[] args) throws Exception {
		XmlMapper xmlMapper = new XmlMapper();
		Config config = xmlMapper.readValue(new File("D:/DW/DataWarehouse/config.xml"), Config.class);

		System.out.println("Host: " + config.database.host);
		System.out.println("Port: " + config.database.port);
		System.out.println("Src Folder Path: " + config.source.source_folder_path);
        System.out.println("Src Transaction Path: " + config.transaction.scripts);
        System.out.println("Src Aggregate Path: " + config.aggregate.scripts);
        System.out.println("Src Aggregate Path: " + config.mart.scripts);

		//EXTRACT
        String url = config.source.source_url;
        Document doc = Jsoup.connect(url).get();

        WeatherData data = Scraper.fetchWeatherData(doc);
		String fileName = Scraper.generateFileName(config.source.source_folder_path);
		Scraper.writeToCSV(data, fileName);
		//LOAD CSV
		loadCsvToStaging.load(fileName);
		//TRANSFORM
		TransformProcess transform = new TransformProcess();
		transform.runTransform(config.transaction.scripts);

		// LOAD WAREHOUSE
		LoadWarehouseProcess loadWH = new  LoadWarehouseProcess();
		loadWH.runLoadWarehouse(List.of("D:\\DW\\DataWarehouse\\database\\warehouse\\proc_load_warehouse.sql"));
	}

}