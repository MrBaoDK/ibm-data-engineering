SELECT ft."stationid", dt."trucktype",
    SUM(ft."wastecollected") AS total_waste_collected
FROM "FactTrips" ft
JOIN "DimTruck" dt ON ft."truckid" = dt."truckid"
GROUP BY GROUPING SETS (
        (ft."stationid", dt."trucktype"),  -- Group by station and truck type
        (ft."stationid"),                  -- Group by station only
        (dt."trucktype"),                  -- Group by truck type only
        ()                                 -- No group, total aggregation
    );

SELECT d."year", st."city", ft."stationid",
    SUM(ft."wastecollected") AS total_waste_collected
FROM "FactTrips" ft
JOIN "DimStation" st ON ft."stationid" = st."stationid"
JOIN "DimDate" d ON ft."dateid" = d."dateid"
GROUP BY ROLLUP (d."year", st."city", ft."stationid");

SELECT d."year", st."city", ft."stationid",
    AVG(ft."wastecollected") AS average_waste_collected
FROM "FactTrips" ft
JOIN "DimStation" st ON ft."stationid" = st."stationid"
JOIN "DimDate" d ON ft."dateid" = d."dateid"
GROUP BY CUBE (d."year", st."city", ft."stationid");

CREATE MATERIALIZED VIEW max_waste_stats AS
SELECT st."city", ft."stationid", tr."trucktype",
    MAX(ft."wastecollected") AS max_waste_collected
FROM "FactTrips" ft
JOIN "DimStation" st ON ft."stationid" = st."stationid"
JOIN "DimTruck" tr ON ft."truckid" = tr."truckid"
GROUP BY st."city", ft."stationid", tr."trucktype";