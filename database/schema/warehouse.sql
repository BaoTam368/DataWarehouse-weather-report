CREATE DATABASE IF NOT EXISTS warehouse CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE warehouse;

-- DimTime: lưu cả datetime + các thành phần (năm/tháng/ngày/giờ/phút)
CREATE TABLE IF NOT EXISTS DimTime
(
    TimeKey      INT AUTO_INCREMENT PRIMARY KEY,
    FullDateTime DATETIME NOT NULL,
    DateOnly     DATE     NOT NULL,
    Year         INT      NOT NULL,
    Quarter      TINYINT  NOT NULL,
    Month        TINYINT  NOT NULL,
    Day          TINYINT  NOT NULL,
    Weekday      VARCHAR(20),
    Hour         TINYINT  NOT NULL,
    Minute       TINYINT  NOT NULL,
    UNIQUE KEY uk_dimtime (FullDateTime)
) ENGINE = InnoDB;

-- DimWind: tách hướng/tốc độ (surrogate key, tránh trùng)
CREATE TABLE IF NOT EXISTS DimWind
(
    WindKey       INT AUTO_INCREMENT PRIMARY KEY,
    WindDirection VARCHAR(10)   NOT NULL,
    WindSpeed     DECIMAL(5, 2) NOT NULL,
    UNIQUE KEY uk_dimwind (WindDirection, WindSpeed)
) ENGINE = InnoDB;

-- DimUV: có thể thêm mức UV nếu muốn
CREATE TABLE IF NOT EXISTS DimUV
(
    UVKey   INT AUTO_INCREMENT PRIMARY KEY,
    UVValue DECIMAL(4, 2) NOT NULL,
    -- Ví dụ phân loại mức UV (optional):
    UVLevel VARCHAR(20) GENERATED ALWAYS AS (
        CASE
            WHEN UVValue IS NULL THEN NULL
            WHEN UVValue < 3 THEN 'Low'
            WHEN UVValue < 6 THEN 'Moderate'
            WHEN UVValue < 8 THEN 'High'
            WHEN UVValue < 11 THEN 'Very High'
            ELSE 'Extreme'
            END
        ) VIRTUAL,
    UNIQUE KEY uk_dimuv (UVValue)
) ENGINE = InnoDB;

-- DimCloud: gom Cloud cover + Cloud ceiling
CREATE TABLE IF NOT EXISTS DimCloud
(
    CloudKey     INT AUTO_INCREMENT PRIMARY KEY,
    CloudCover   DECIMAL(5, 2) NOT NULL,
    CloudCeiling INT           NULL,
    UNIQUE KEY uk_dimcloud (CloudCover, CloudCeiling)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS FactWeather
(
    WeatherKey  BIGINT AUTO_INCREMENT PRIMARY KEY,
    TimeKey     INT NOT NULL,
    WindKey     INT,
    UVKey       INT,
    CloudKey    INT,

    Temperature DECIMAL(5, 2),
    Humidity    DECIMAL(5, 2),
    DewPoint    DECIMAL(5, 2),
    Pressure    DECIMAL(6, 2),
    Visibility  DECIMAL(5, 2),

    CONSTRAINT fk_fact_time FOREIGN KEY (TimeKey) REFERENCES DimTime (TimeKey),
    CONSTRAINT fk_fact_wind FOREIGN KEY (WindKey) REFERENCES DimWind (WindKey),
    CONSTRAINT fk_fact_uv FOREIGN KEY (UVKey) REFERENCES DimUV (UVKey),
    CONSTRAINT fk_fact_cloud FOREIGN KEY (CloudKey) REFERENCES DimCloud (CloudKey)
) ENGINE = InnoDB;
