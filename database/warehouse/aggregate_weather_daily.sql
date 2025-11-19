USE warehouse;

-- Tạo bảng AggregateWeatherDaily: Để
CREATE TABLE IF NOT EXISTS AggregateWeatherDaily
(
    DateOnly    DATE PRIMARY KEY,
    AvgTemp     DECIMAL(5, 2),
    MinTemp     DECIMAL(5, 2),
    MaxTemp     DECIMAL(5, 2),
    AvgHumidity DECIMAL(5, 2),
    AvgPressure DECIMAL(6, 2),
    RowCount    INT
);

REPLACE INTO AggregateWeatherDaily
SELECT dt.DateOnly,
       ROUND(AVG(f.Temperature), 2) AS AvgTemp,
       ROUND(MIN(f.Temperature), 2) AS MinTemp,
       ROUND(MAX(f.Temperature), 2) AS MaxTemp,
       ROUND(AVG(f.Humidity), 2)    AS AvgHumidity,
       ROUND(AVG(f.Pressure), 2)    AS AvgPressure,
       COUNT(*)                     AS RowCount
FROM warehouse.FactWeather f
         JOIN warehouse.DimTime dt ON dt.TimeKey = f.TimeKey
GROUP BY dt.DateOnly
ORDER BY dt.DateOnly;