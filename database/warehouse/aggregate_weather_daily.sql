USE datawarehouse;

-- Tạo bảng aggregate_weather_daily nếu chưa tồn tại
CREATE TABLE IF NOT EXISTS aggregate_weather_daily
(
    DateOnly    DATE PRIMARY KEY,
    AvgTemp     DECIMAL(5, 2),
    MinTemp     DECIMAL(5, 2),
    MaxTemp     DECIMAL(5, 2),
    AvgHumidity DECIMAL(5, 2),
    AvgPressure DECIMAL(6, 2),
    RowCount    INT
);

-- Tính toán aggregate theo ngày từ FactWeather + date_dim
REPLACE INTO aggregate_weather_daily
SELECT
    d.DateOnly,
    ROUND(AVG(f.Temperature), 2) AS AvgTemp,
    ROUND(MIN(f.Temperature), 2) AS MinTemp,
    ROUND(MAX(f.Temperature), 2) AS MaxTemp,
    ROUND(AVG(f.Humidity), 2)    AS AvgHumidity,
    ROUND(AVG(f.Pressure), 2)    AS AvgPressure,
    COUNT(*)                     AS RowCount
FROM datawarehouse.FactWeather f
         JOIN datawarehouse.date_dim d
              ON d.SK = f.SK
GROUP BY d.DateOnly
ORDER BY d.DateOnly;