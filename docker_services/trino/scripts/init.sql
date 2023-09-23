CREATE SCHEMA IF NOT EXISTS delta.bronze WITH (location = 's3a://bronze/');
CREATE SCHEMA IF NOT EXISTS delta.silver WITH (location = 's3a://silver/');
CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3a://gold/');
CREATE SCHEMA IF NOT EXISTS delta.tmp WITH (location = 's3a://tmp/');
SHOW SCHEMAS IN delta;

--SHOW TABLES IN hive.iris;
--
--DROP TABLE hive.delta_lake.results;
--
--create table hive.delta_lake.results
--(
--  dt varchar
-- ,home_teamName varchar
-- ,away_teamName varchar
-- ,home_scoreHome varchar
-- ,away_scoreAway varchar
-- ,tournamentName varchar
-- ,cityCity varchar
-- ,countryCountry varchar
-- ,neutralTRUE varchar
--)
--with
--(EXTERNAL_LOCATION = 's3a://delta-lake/demo2', FORMAT = 'PARQUET', PARTITIONED_BY = ARRAY['countryCountry']);
--
--SELECT * FROM delta.delta_lake.results LIMIT 10;