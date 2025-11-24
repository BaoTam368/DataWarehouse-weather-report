use datawarehouse;

CREATE procedure proc_load_warehouse()
begin

    -- =====================================
    -- 1 DIM TIME
    -- =====================================
    INSERT INTO datawarehouse.DimTime (FullDate, Day, Month, Year, Weekday)
SELECT newrow.FullDate,
       newrow.Day,
       newrow.Month,
       newrow.Year,
       newrow.Weekday
FROM (
    SELECT DISTINCT
        STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s') AS FullDate,
        DAY(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s')) AS Day,
        MONTH(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s')) AS Month,
        YEAR(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s')) AS Year,
        DAYNAME(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s')) AS Weekday
    FROM staging.stg_weather_clean s
) AS newrow
ON DUPLICATE KEY UPDATE
    Weekday = newrow.Weekday;
    
    -- =====================================
    -- 2 LOAD DATE_DIM (LẤY TỪ DIMTIME)
    -- =====================================
    INSERT INTO date_dim (DateOnly,Day, Month, Year, Weekday)
SELECT
    newrow.DateOnly,
    DAY(newrow.DateOnly),
    MONTH(newrow.DateOnly),
    YEAR(newrow.DateOnly),
    DAYNAME(newrow.DateOnly)
FROM (
    SELECT DISTINCT
        DATE(t.FullDate) AS DateOnly
    FROM datawarehouse.DimTime t
) AS newrow
WHERE NOT EXISTS (
    SELECT 1 FROM date_dim d
    WHERE d.DateOnly = newrow.DateOnly
);


    -- =====================================
    -- 3 DIM WIND
    -- =====================================
    INSERT INTO datawarehouse.DimWind (Direction, Speed)
    SELECT newrow.Direction, newrow.Speed
    FROM (
    SELECT DISTINCT
        s.WindDirection AS Direction,
        s.WindSpeed + 0 AS Speed
    FROM staging.stg_weather_clean s
    ) AS newrow
    ON DUPLICATE KEY UPDATE
    Speed = newrow.Speed;

    -- =====================================
    -- 4 DIM UV
    -- =====================================
    INSERT INTO datawarehouse.DimUV (UVValue, UVLevel)
    SELECT newrow.UVValue, newrow.UVLevel
    FROM (
    SELECT DISTINCT
        SUBSTRING_INDEX(TRIM(s.UVValue), ' ', 1) + 0 AS UVValue,
        TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(s.UVValue,'(', -1), ')', 1)) AS UVLevel
    FROM staging.stg_weather_clean s
    ) AS newrow
    ON DUPLICATE KEY UPDATE
    UVLevel = newrow.UVLevel;
    -- =====================================
    -- 5 FACT WEATHER
    -- =====================================

INSERT INTO datawarehouse.FactWeather (
    SK, Day, WindKey, UVKey,
    Temperature, Humidity, DewPoint, Pressure,
    CloudCover, Visibility, CloudCeiling
)
SELECT 
    newrow.SK,
    newrow.Day,
    newrow.WindKey,
    newrow.UVKey,
    newrow.Temperature,
    newrow.Humidity,
    newrow.DewPoint,
    newrow.Pressure,
    newrow.CloudCover,
    newrow.Visibility,
    newrow.CloudCeiling
FROM (
    SELECT
        d.SK,
        -- Day phải lấy từ STAGING:
        DAY(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s')) AS Day,
        w.WindKey,
        u.UVKey,
        CAST(REGEXP_REPLACE(s.Temperature, '[^0-9.-]', '') AS DECIMAL(4,1)) AS Temperature,
        CAST(REGEXP_REPLACE(s.Humidity, '[^0-9.-]', '') AS DECIMAL(4,1)) AS Humidity,
        CAST(REGEXP_REPLACE(s.DewPoint, '[^0-9.-]', '') AS DECIMAL(4,1)) AS DewPoint,
        CAST(REGEXP_REPLACE(s.Pressure, '[^0-9.-]', '') AS DECIMAL(6,2)) AS Pressure,
        CAST(REGEXP_REPLACE(s.CloudCover, '[^0-9.-]', '') AS DECIMAL(5,2)) AS CloudCover,
        CAST(REGEXP_REPLACE(s.Visibility, '[^0-9.-]', '') AS DECIMAL(5,2)) AS Visibility,
        CAST(REGEXP_REPLACE(s.CloudCeiling, '[^0-9.-]', '') AS SIGNED) AS CloudCeiling
    FROM staging.stg_weather_clean s
    JOIN datawarehouse.date_dim d
        ON d.DateOnly = DATE(STR_TO_DATE(s.FullDate,'%Y-%m-%d %H:%i:%s'))
    JOIN datawarehouse.DimWind w
        ON w.Direction = s.WindDirection
        AND w.Speed = s.WindSpeed + 0

    JOIN datawarehouse.DimUV u
        ON u.UVValue = SUBSTRING_INDEX(TRIM(s.UVValue), ' ', 1) + 0
      AND u.UVLevel = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(s.UVValue,'(', -1), ')', 1))
) AS newrow
ON DUPLICATE KEY UPDATE
    Day          = newrow.Day,
    WindKey      = newrow.WindKey,
    UVKey        = newrow.UVKey,
    Temperature  = newrow.Temperature,
    Humidity     = newrow.Humidity,
    DewPoint     = newrow.DewPoint,
    Pressure     = newrow.Pressure,
    CloudCover   = newrow.CloudCover,
    Visibility   = newrow.Visibility,
    CloudCeiling = newrow.CloudCeiling;

    -- =====================================
    -- 6 CLEAN STAGING
    -- =====================================
    TRUNCATE TABLE staging.stg_weather;
    TRUNCATE TABLE staging.stg_weather_clean;
end;

