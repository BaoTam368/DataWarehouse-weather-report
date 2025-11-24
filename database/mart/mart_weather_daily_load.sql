USE mart_weather;

-- (tuỳ chọn) Tạo bảng nếu chưa có
CREATE TABLE IF NOT EXISTS WeatherDailySummary
(
    DateOnly     DATE PRIMARY KEY,
    AvgTemp      DECIMAL(5, 2),
    MinTemp      DECIMAL(5, 2),
    MaxTemp      DECIMAL(5, 2),
    AvgHumidity  DECIMAL(5, 2),
    AvgPressure  DECIMAL(6, 2),
    TempCategory VARCHAR(20)
);

REPLACE INTO WeatherDailySummary
SELECT a.DateOnly,
       a.AvgTemp,
       a.MinTemp,
       a.MaxTemp,
       a.AvgHumidity,
       a.AvgPressure,
       CASE
           WHEN a.AvgTemp IS NULL THEN NULL
           WHEN a.AvgTemp < 15 THEN 'Cold'
           WHEN a.AvgTemp <= 25 THEN 'Mild'
           ELSE 'Hot'
           END AS TempCategory
FROM datawarehouse.aggregate_weather_daily a;