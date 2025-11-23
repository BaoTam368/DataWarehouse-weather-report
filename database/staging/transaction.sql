use staging;

INSERT INTO stg_weather_clean (
    FullDate,
    Weekday,
    DateValue,
    Temperature,
    UVValue,
    Wind,
    Humidity,
    DewPoint,
    Pressure,
    CloudCover,
    Visibility,
    CloudCeiling
)
SELECT
    -- Convert ngày giờ từ varchar → DATETIME
    STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') AS FullDate,

    -- Copy toàn bộ dữ liệu thô (được trim)
    TRIM(Weekday)        AS Weekday,
    TRIM(Date)            AS DateValue,
    TRIM(Temperature)     AS Temperature,
    TRIM(UVValue)         AS UVValue,
    TRIM(Wind)            AS Wind,
    TRIM(Humidity)        AS Humidity,
    TRIM(DewPoint)        AS DewPoint,
    TRIM(Pressure)        AS Pressure,
    TRIM(CloudCover)      AS CloudCover,
    TRIM(Visibility)      AS Visibility,
    TRIM(CloudCeiling)    AS CloudCeiling

FROM staging.stg_weather
WHERE STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL;