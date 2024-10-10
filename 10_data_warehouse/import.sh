export PGPASSWORD=ZdiF5EjxGOTVSE1OZxSoN4gO
export PGHOST=172.21.137.242
export PGPORT=5432
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimDate.csv
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimTruck.csv
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/DimStation.csv
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0260EN-SkillsNetwork/labs/Final%20Assignment/FactTrips.csv
psql --host $PGHOST -p $PGPORT -U postgres < dwh_ddl.sql
psql --host $PGHOST -p $PGPORT -U postgres -c "\\copy \"DimDate\" from 'DimDate.csv' DELIMITER ',' CSV HEADER"
psql --host $PGHOST -p $PGPORT -U postgres -c "\\copy \"DimStation\" from 'DimStation.csv' DELIMITER ',' CSV HEADER"
psql --host $PGHOST -p $PGPORT -U postgres -c "\\copy \"DimTruck\" from 'DimTruck.csv' DELIMITER ',' CSV HEADER"
psql --host $PGHOST -p $PGPORT -U postgres -c "\\copy \"FactTrips\" from 'FactTrips.csv' DELIMITER ',' CSV HEADER"