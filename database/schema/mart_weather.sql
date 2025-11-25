CREATE DATABASE IF NOT EXISTS mart_weather
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS mart_weather.WeatherDailySummary (
  DateOnly DATE PRIMARY KEY,
  AvgTemp DECIMAL(5,2),
  MinTemp DECIMAL(5,2),
  MaxTemp DECIMAL(5,2),
  AvgHumidity DECIMAL(5,2),
  AvgPressure DECIMAL(6,2),
  TempCategory VARCHAR(20)
);
