-- Table for MyDimDate
CREATE TABLE "MyDimDate" (
    "dateid" INT PRIMARY KEY,
    "date" DATE,
    "month" INT,
    "monthname" VARCHAR(10),
    "quarter" INT,
    "quartername" VARCHAR(5),
    "year" INT,
    "weekday" INT,
    "weekdayname" VARCHAR(10),
    "day" INT
);

-- Table for MyDimWaste
CREATE TABLE "MyDimWaste" (
    "wasteid" INT PRIMARY KEY,
    "wastetype" VARCHAR(15)
);

-- Table for MyDimZone
CREATE TABLE "MyDimZone" (
    "zoneid" INT PRIMARY KEY,
    "zonename" VARCHAR(10),
    "city" VARCHAR(20)
);

-- Table for MyFactTrips
CREATE TABLE "MyFactTrips" (
    "tripid" INT PRIMARY KEY,
    "dateid" INT,
    "zoneid" INT,
    "wasteid" INT,
    "wastecollectedintons" FLOAT,
    FOREIGN KEY ("dateid") REFERENCES "MyDimDate" ("dateid"),
    FOREIGN KEY ("zoneid") REFERENCES "MyDimZone" ("zoneid"),
    FOREIGN KEY ("wasteid") REFERENCES "MyDimWaste" ("wasteid")
);