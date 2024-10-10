DROP TABLE IF EXISTS "FactTrips" CASCADE;
DROP TABLE IF EXISTS "DimDate" CASCADE;
DROP TABLE IF EXISTS "DimTruck" CASCADE;
DROP TABLE IF EXISTS "DimStation" CASCADE;

-- Table for DimDate
CREATE TABLE "DimDate" (
    "dateid" INT PRIMARY KEY,
    "date" DATE,
    "year" INT,
    "quarter" INT,
    "quartername" VARCHAR(50),
    "month" INT,
    "monthname" VARCHAR(50),
    "day" INT,
    "weekday" INT,
    "weekdayname" VARCHAR(50)
);

-- Table for DimTruck
CREATE TABLE "DimTruck" (
    "truckid" INT PRIMARY KEY,
    "trucktype" VARCHAR(100)
);

-- Table for DimStation
CREATE TABLE "DimStation" (
    "stationid" INT PRIMARY KEY,
    "city" VARCHAR(100)
);

-- Table for FactTrips
CREATE TABLE "FactTrips" (
    "tripid" INT PRIMARY KEY,
    "dateid" INT,
    "stationid" INT,
    "truckid" INT,
    "wastecollected" DECIMAL(10, 2),
    FOREIGN KEY ("dateid") REFERENCES "DimDate" ("dateid"),
    FOREIGN KEY ("stationid") REFERENCES "DimStation" ("stationid"),
    FOREIGN KEY ("truckid") REFERENCES "DimTruck" ("truckid")
);