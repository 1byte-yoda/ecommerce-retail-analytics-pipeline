-- Need to manually run each query now.

CREATE SCHEMA IF NOT EXISTS hive.delta_lake
WITH (location = 's3a://delta-lake/');

-- Path s3a://iris/iris_data is the holding directory. We dont give full file path. Only parent directory
CREATE TABLE IF NOT EXISTS hive.iris.iris_data (
  sepal_length DOUBLE,
  sepal_width  DOUBLE,
  petal_length DOUBLE,
  petal_width  DOUBLE,
  class        VARCHAR
)
WITH (
  external_location = 's3a://iris/iris_data',
  format = 'PARQUET'
);

-- Testing
SELECT 
  sepal_length,
  class
FROM hive.iris.iris_data
LIMIT 10;

SHOW TABLES IN hive.iris;

DROP TABLE hive.delta_lake.results;

create table hive.delta_lake.results
(
  dt varchar
 ,home_teamName varchar
 ,away_teamName varchar
 ,home_scoreHome varchar
 ,away_scoreAway varchar
 ,tournamentName varchar
 ,cityCity varchar
 ,countryCountry varchar
 ,neutralTRUE varchar
)
with
(EXTERNAL_LOCATION = 's3a://delta-lake/demo2', FORMAT = 'PARQUET', PARTITIONED_BY = ARRAY['countryCountry']);

SELECT * FROM hive.delta_lake.results_partitioned LIMIT 10;