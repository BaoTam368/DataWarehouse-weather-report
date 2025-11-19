use mart_weather;

REPLACE INTO mart_weather.WeatherDailySummary
SELECT
  a.DateOnly,
  a.AvgTemp, a.MinTemp, a.MaxTemp, a.AvgHumidity, a.AvgPressure,
  CASE
    WHEN a.AvgTemp IS NULL      THEN NULL
    WHEN a.AvgTemp < 15         THEN 'Cold'
    WHEN a.AvgTemp <= 25        THEN 'Mild'
    ELSE 'Hot'
  END AS TempCategory
FROM warehouse.AggregateWeatherDaily a;