CREATE DATABASE IF NOT EXISTS staging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE staging;

create table temp
(
    FullDate     varchar(20),
    Weekday      varchar(20),
    Day         varchar(20),
    Temperature  varchar(20),
    UVValue      varchar(20),
    WindDirection         varchar(20),
    Humidity     varchar(20),
    DewPoint     varchar(20),
    Pressure     varchar(20),
    Cloud        varchar(20),
    Visibility   varchar(20),
    CloudCeiling varchar(20)
);

CREATE TABLE official
(
    FullDate      DATETIME,
    Weekday       VARCHAR(20),
    Day          VARCHAR(20),
    Temperature   DECIMAL(5, 2),
    UVValue       DECIMAL(4, 2),
    UVLevel       VARCHAR(20),
    WindDirection VARCHAR(10),
    WindSpeed     DECIMAL(5, 2),
    Humidity      DECIMAL(5, 2),
    DewPoint      DECIMAL(5, 2),
    Pressure      DECIMAL(6, 2),
    Cloud         DECIMAL(5, 2),
    Visibility    DECIMAL(5, 2),
    CloudCeiling  INT
);