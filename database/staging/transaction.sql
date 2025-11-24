USE staging;

TRUNCATE TABLE official;

INSERT INTO official
(FullDate, Weekday, `Day`,
 Temperature, UVValue, UVLevel, WindDirection, WindSpeed,
 Humidity, DewPoint, Pressure, Cloud,
 Visibility, CloudCeiling)
SELECT
    STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s'),

    Weekday,
    `Day`,

    -- Temperature
    CAST(REGEXP_SUBSTR(Temperature, '[0-9]+(\\.[0-9]+)?') AS DECIMAL(5,2)),

    -- UVValue (số)
    CAST(REGEXP_SUBSTR(UVValue, '[0-9]+(\\.[0-9]+)?') AS DECIMAL(4,2)),

    -- UVLevel (text sau số)
    TRIM(
            REGEXP_REPLACE(UVValue, '^[0-9]+(\\.[0-9]+)?\\s*', '')
    ),

    -- Wind
    SUBSTRING_INDEX(WindDirection, ' ', 1),
    CAST(REGEXP_SUBSTR(WindDirection, '[0-9]+(\\.[0-9]+)?') AS DECIMAL(5,2)),

    -- Other numeric fields
    CAST(REGEXP_SUBSTR(Humidity, '[0-9]+') AS DECIMAL(5,2)),
    CAST(REGEXP_SUBSTR(DewPoint, '[0-9]+') AS DECIMAL(5,2)),
    CAST(REGEXP_SUBSTR(Pressure, '[0-9]+') AS DECIMAL(6,2)),
    CAST(REGEXP_SUBSTR(Cloud, '[0-9]+') AS DECIMAL(5,2)),
    CAST(REGEXP_SUBSTR(Visibility, '[0-9]+') AS DECIMAL(5,2)),
    CAST(REGEXP_SUBSTR(CloudCeiling, '[0-9]+') AS SIGNED)
FROM temp
WHERE STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL;