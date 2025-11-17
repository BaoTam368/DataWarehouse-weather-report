CREATE DATABASE IF NOT EXISTS control
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE control;

CREATE TABLE config_source(
	source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(100),
    source_url VARCHAR(1000),
    source_file_location VARCHAR(100),
    file_format VARCHAR(200),   
	scraping_script_path VARCHAR(500),
    destination_staging VARCHAR(100),
    transform_procedure VARCHAR(100),
    load_warehouse_procedure VARCHAR(100),
    aggregate_procedure VARCHAR(100),
    aggregate_table VARCHAR(100),
	aggregate_file_path VARCHAR(500)
);

CREATE TABLE config_mart(
	mart_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100),
    remote_host VARCHAR(100),
    passwword VARCHAR(200),
    file_format VARCHAR(200),
    aggregate_file_path VARCHAR(500),
	load_mart_script_path VARCHAR(500)
);

CREATE TABLE file_log(
	file_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT,
    file_path VARCHAR(1000),
    time datetime,
    count INT,
    size double,
	status VARCHAR(20),
    execute_time datetime,
    CONSTRAINT fk_file_source
        FOREIGN KEY (source_id)
        REFERENCES config_source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

CREATE TABLE process_log(
	process_id INT AUTO_INCREMENT PRIMARY KEY,
    source_id INT,
    process_code VARCHAR(20),
    process_name VARCHAR(2000),
    started_at datetime,
    status VARCHAR(20),
	updated_at datetime,
    CONSTRAINT fk_process_source
        FOREIGN KEY (source_id)
        REFERENCES config_source(source_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);