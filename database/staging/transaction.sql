USE staging;

TRUNCATE TABLE official;

INSERT INTO official
(FullDate, Weekday, `Day`,
 Temperature, UVValue, WindDirection, WindSpeed,
 Humidity, DewPoint, Pressure, Cloud,
 Visibility, CloudCeiling)
SELECT STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') AS FullDate,                    -- Convert string to date and time format
       Weekday,                                                                   -- Weekday as is (Thu, Fri, Sat, Sun, Mon, Tue, Wed)
       `Day`,
       CAST(REGEXP_SUBSTR(Temperature, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)), -- Extract numeric part of Temperature and convert to DECIMAL
       CAST(REGEXP_SUBSTR(UVValue, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(4, 2)),     -- Extract numeric part of UVValue and convert to DECIMAL
       SUBSTRING_INDEX(Wind, ' ', 1),
       CAST(REGEXP_SUBSTR(Wind, '[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)),          -- Extract numeric part of WindSpeed and convert to DECIMAL
       CAST(REGEXP_SUBSTR(Humidity, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)),    -- Extract numeric part of Humidity and convert to DECIMAL
       CAST(REGEXP_SUBSTR(DewPoint, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)),    -- Extract numeric part of DewPoint and convert to DECIMAL
       CAST(REGEXP_SUBSTR(Pressure, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(6, 2)),    -- Extract numeric part of Pressure and convert to DECIMAL
       CAST(REGEXP_SUBSTR(Cloud, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)),       -- Extract numeric part of Cloud and convert to DECIMAL
       CAST(REGEXP_SUBSTR(Visibility, '-?[0-9]+(\\.[0-9]+)?') AS DECIMAL(5, 2)),  -- Extract numeric part of Visibility and convert to DECIMAL
       CAST(REGEXP_SUBSTR(CloudCeiling, '[0-9]+') AS SIGNED)                      -- Extract numeric part of CloudCeiling and convert to SIGNED INTEGER (positive or negative)
FROM temp
WHERE STR_TO_DATE(FullDate, '%Y-%m-%d %H:%i:%s') IS NOT NULL;
-- Ensure only valid date entries are inserted

-- note: -?[0-9]+(\\.[0-9]+)? is a regular expression that matches any number, including negative numbers, 
-- with optional decimal points. The -? allows for an optional negative sign at the beginning.
-- [0-9]+ matches one or more digits.
-- (\\.[0-9]+)? matches an optional decimal point followed by one or more digits.
-- which mean a number can be an integer or a decimal, and it can be positive or negative.