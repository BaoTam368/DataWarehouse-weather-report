INSERT INTO staging.stg_weather_clean (
    FullDate, Weekday, Day,
    Temperature, UVValue, UVLevel,
    WindDirection, WindSpeed,
    Humidity, DewPoint, Pressure,
    CloudCover, Visibility, CloudCeiling
)

SELECT
    STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') AS FullDate,
    Weekday,
    CONCAT(
        DAY(STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s')),
        ' tháng ',
        MONTH(STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s'))
    ) AS Day,

    -- Temperature numeric
    REGEXP_SUBSTR(Temperature, '[0-9.]+') AS Temperature,

    -- UV numeric part: before the first space
    LEFT(UVValue, LOCATE(' ', UVValue) - 1) AS UVValue,

    -- UV level (inside parentheses) – NO lookbehind
    TRIM(BOTH '()' FROM SUBSTRING_INDEX(UVValue, '(', -1)) AS UVLevel,

    -- Wind direction = first word (letters)
    SUBSTRING_INDEX(Wind, ' ', 1) AS WindDirection,

    -- Wind speed = number
    REGEXP_SUBSTR(Wind, '[0-9.]+') AS WindSpeed,

    REGEXP_SUBSTR(Humidity, '[0-9.]+') AS Humidity,
    REGEXP_SUBSTR(DewPoint, '[0-9.]+') AS DewPoint,
    REGEXP_SUBSTR(Pressure, '[0-9.]+') AS Pressure,
    REGEXP_SUBSTR(CloudCover, '[0-9.]+') AS CloudCover,
    REGEXP_SUBSTR(Visibility, '[0-9.]+') AS Visibility,
    REGEXP_SUBSTR(CloudCeiling, '[0-9.]+') AS CloudCeiling

FROM staging.stg_weather
WHERE STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL;
