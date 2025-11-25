use datawarehouse;


CREATE TABLE date_dim (
    SK INT AUTO_INCREMENT PRIMARY KEY,
    DateOnly DATE NOT null UNIQUE,
    Day INT,
    Month INT,
    Year INT,
    Weekday VARCHAR(20)
);

CREATE TABLE DimTime (
    FullDate DATETIME NOT NULL UNIQUE,
    Day INT,
    Month INT,
    Year INT,
    Weekday VARCHAR(20)
);

CREATE TABLE DimWind (
    WindKey INT auto_increment PRIMARY KEY,
    Direction NVARCHAR(10),            
    Speed DECIMAL(5,2)                
);

CREATE TABLE DimUV (
    UVKey INT auto_increment PRIMARY KEY,
    UVValue DECIMAL(4,2),             
    UVLevel NVARCHAR(20)               
);


CREATE TABLE datawarehouse.FactWeather (
    SK INT PRIMARY KEY,
    Day VARCHAR(50),
    WindKey INT,
    UVKey INT,
    Temperature DECIMAL(4,1),
    Humidity DECIMAL(4,1),
    DewPoint DECIMAL(4,1),
    Pressure DECIMAL(6,2),
    CloudCover DECIMAL(5,2),
    Visibility DECIMAL(5,2),
    CloudCeiling INT,
    FOREIGN KEY (SK) REFERENCES datawarehouse.date_dim(SK),
    FOREIGN KEY (WindKey) REFERENCES datawarehouse.DimWind(WindKey),
    FOREIGN KEY (UVKey) REFERENCES datawarehouse.DimUV(UVKey)
);


