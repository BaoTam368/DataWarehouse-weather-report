package com.example.DataWarehouse;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.*;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import config.Config;
import java.io.File;

import extract.*;

public class Test {

	public static void main(String[] args) throws Exception {
		XmlMapper xmlMapper = new XmlMapper();
		Config config = xmlMapper.readValue(new File("config.xml"), Config.class);

		System.out.println("Host: " + config.database.host);
		System.out.println("Port: " + config.database.port);
		System.out.println("Src Folder Path: " + config.source.source_folder_path);

		WeatherData data = Scraper.fetchWeatherData(config.source.source_url);
		String fileName = Scraper.generateFileName(config.source.source_folder_path);
		Scraper.writeToCSV(data, fileName);
		
	}

}
