# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # **AT2: Data processing**
# MAGIC
# MAGIC The goal of this assignment is to analyse a large dataset using Databricks Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ##  *PART 1: Data Ingestion and Preparation*![](path)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Explore Data Set 
# MAGIC
# MAGIC Firstly, I downloaded datasets. All datasets contain Catalog --> Volume file. 
# MAGIC Then, I analysed the dataset according required: 
# MAGIC - a.	Trips finishing before the starting time
# MAGIC - b.	Trips where the pickup/dropoff datetime is outside of the range
# MAGIC - c.	Trips with negative speed
# MAGIC - d.	Trips with very high speed (look for NYC and outside of NYC speed limit )
# MAGIC - e.	Trips that are travelling too short or too long (duration wise)
# MAGIC - f.	Trips that are travelling too short or too long (distance wise)
# MAGIC - g.	Any other logic you think is important
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW yellow_raw
# MAGIC USING PARQUET
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/workspace/bde/assignment2/yellow/yellow_taxi.parquet'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW green_raw
# MAGIC USING PARQUET
# MAGIC OPTIONS (
# MAGIC   path '/Volumes/workspace/bde/assignment2/green/green_taxi.parquet'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS yellow_rows
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS green_rows
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM yellow_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM green_raw
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(tpep_pickup_datetime)  AS min_pickup,
# MAGIC     MAX(tpep_pickup_datetime)  AS max_pickup,
# MAGIC     MIN(tpep_dropoff_datetime) AS min_dropoff,
# MAGIC     MAX(tpep_dropoff_datetime) AS max_dropoff
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(lpep_pickup_datetime)  AS min_pickup,
# MAGIC     MAX(lpep_pickup_datetime)  AS max_pickup,
# MAGIC     MIN(lpep_dropoff_datetime) AS min_dropoff,
# MAGIC     MAX(lpep_dropoff_datetime) AS max_dropoff
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS invalid_time_order,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE WHEN
# MAGIC             tpep_pickup_datetime < '2014-01-01'
# MAGIC             OR tpep_pickup_datetime >= '2025-01-01'
# MAGIC             OR tpep_dropoff_datetime < '2014-01-01'
# MAGIC             OR tpep_dropoff_datetime >= '2025-01-01'
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS outside_date_range
# MAGIC
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE WHEN lpep_dropoff_datetime <= lpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS invalid_time_order,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE WHEN
# MAGIC             lpep_pickup_datetime < '2014-01-01'
# MAGIC             OR lpep_pickup_datetime >= '2025-01-01'
# MAGIC             OR lpep_dropoff_datetime < '2014-01-01'
# MAGIC             OR lpep_dropoff_datetime >= '2025-01-01'
# MAGIC         THEN 1 ELSE 0 END
# MAGIC     ) AS outside_date_range
# MAGIC
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0
# MAGIC     ) AS min_duration_minutes,
# MAGIC
# MAGIC     MAX(
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0
# MAGIC     ) AS max_duration_minutes,
# MAGIC
# MAGIC     MIN(trip_distance) AS min_distance_miles,
# MAGIC     MAX(trip_distance) AS max_distance_miles
# MAGIC
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0
# MAGIC     ) AS min_duration_minutes,
# MAGIC
# MAGIC     MAX(
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0
# MAGIC     ) AS max_duration_minutes,
# MAGIC
# MAGIC     MIN(trip_distance) AS min_distance_miles,
# MAGIC     MAX(trip_distance) AS max_distance_miles
# MAGIC
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     percentile_approx(
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0,
# MAGIC         array(0.01, 0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS duration_percentiles_minutes,
# MAGIC
# MAGIC     percentile_approx(
# MAGIC         trip_distance,
# MAGIC         array(0.01, 0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS distance_percentiles_miles
# MAGIC
# MAGIC FROM yellow_raw
# MAGIC WHERE
# MAGIC     tpep_dropoff_datetime > tpep_pickup_datetime
# MAGIC     AND tpep_pickup_datetime >= '2014-01-01'
# MAGIC     AND tpep_pickup_datetime < '2025-01-01'
# MAGIC     AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC     AND tpep_dropoff_datetime < '2025-01-01'
# MAGIC     AND trip_distance >= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     percentile_approx(
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0,
# MAGIC         array(0.01, 0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS duration_percentiles_minutes,
# MAGIC
# MAGIC     percentile_approx(
# MAGIC         trip_distance,
# MAGIC         array(0.01, 0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS distance_percentiles_miles
# MAGIC
# MAGIC FROM green_raw
# MAGIC WHERE
# MAGIC     lpep_dropoff_datetime > lpep_pickup_datetime
# MAGIC     AND lpep_pickup_datetime >= '2014-01-01'
# MAGIC     AND lpep_pickup_datetime < '2025-01-01'
# MAGIC     AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC     AND lpep_dropoff_datetime < '2025-01-01'
# MAGIC     AND trip_distance >= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_speed AS (
# MAGIC     SELECT
# MAGIC         trip_distance,
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 3600.0 AS duration_hours,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         ((unix_timestamp(tpep_dropoff_datetime) -
# MAGIC           unix_timestamp(tpep_pickup_datetime)) / 3600.0) AS speed_kmh
# MAGIC
# MAGIC     FROM yellow_raw
# MAGIC
# MAGIC     WHERE
# MAGIC         tpep_dropoff_datetime > tpep_pickup_datetime
# MAGIC         AND tpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND tpep_pickup_datetime < '2025-01-01'
# MAGIC         AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND tpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(speed_kmh) AS min_speed_kmh,
# MAGIC     MAX(speed_kmh) AS max_speed_kmh,
# MAGIC
# MAGIC     SUM(CASE WHEN speed_kmh < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_speed_count,
# MAGIC
# MAGIC     percentile_approx(
# MAGIC         speed_kmh,
# MAGIC         array(0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS speed_percentiles_kmh
# MAGIC
# MAGIC FROM yellow_speed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_speed AS (
# MAGIC     SELECT
# MAGIC         trip_distance,
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 3600.0 AS duration_hours,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         ((unix_timestamp(lpep_dropoff_datetime) -
# MAGIC           unix_timestamp(lpep_pickup_datetime)) / 3600.0) AS speed_kmh
# MAGIC
# MAGIC     FROM green_raw
# MAGIC
# MAGIC     WHERE
# MAGIC         lpep_dropoff_datetime > lpep_pickup_datetime
# MAGIC         AND lpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND lpep_pickup_datetime < '2025-01-01'
# MAGIC         AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND lpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     MIN(speed_kmh) AS min_speed_kmh,
# MAGIC     MAX(speed_kmh) AS max_speed_kmh,
# MAGIC
# MAGIC     SUM(CASE WHEN speed_kmh < 0 THEN 1 ELSE 0 END)
# MAGIC         AS negative_speed_count,
# MAGIC
# MAGIC     percentile_approx(
# MAGIC         speed_kmh,
# MAGIC         array(0.50, 0.95, 0.99, 0.999)
# MAGIC     ) AS speed_percentiles_kmh
# MAGIC
# MAGIC FROM green_speed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_speed AS (
# MAGIC     SELECT
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         ((unix_timestamp(tpep_dropoff_datetime) -
# MAGIC           unix_timestamp(tpep_pickup_datetime)) / 3600.0) AS speed_kmh
# MAGIC     FROM yellow_raw
# MAGIC     WHERE
# MAGIC         tpep_dropoff_datetime > tpep_pickup_datetime
# MAGIC         AND tpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND tpep_pickup_datetime < '2025-01-01'
# MAGIC         AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND tpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS valid_duration_rows,
# MAGIC     SUM(CASE WHEN speed_kmh > 100 THEN 1 ELSE 0 END) AS over_100_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 120 THEN 1 ELSE 0 END) AS over_120_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 150 THEN 1 ELSE 0 END) AS over_150_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 200 THEN 1 ELSE 0 END) AS over_200_kmh
# MAGIC FROM yellow_speed;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_speed AS (
# MAGIC     SELECT
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         ((unix_timestamp(lpep_dropoff_datetime) -
# MAGIC           unix_timestamp(lpep_pickup_datetime)) / 3600.0) AS speed_kmh
# MAGIC     FROM green_raw
# MAGIC     WHERE
# MAGIC         lpep_dropoff_datetime > lpep_pickup_datetime
# MAGIC         AND lpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND lpep_pickup_datetime < '2025-01-01'
# MAGIC         AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND lpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS valid_duration_rows,
# MAGIC     SUM(CASE WHEN speed_kmh > 100 THEN 1 ELSE 0 END) AS over_100_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 120 THEN 1 ELSE 0 END) AS over_120_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 150 THEN 1 ELSE 0 END) AS over_150_kmh,
# MAGIC     SUM(CASE WHEN speed_kmh > 200 THEN 1 ELSE 0 END) AS over_200_kmh
# MAGIC FROM green_speed;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_duration AS (
# MAGIC     SELECT
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_minutes
# MAGIC     FROM yellow_raw
# MAGIC     WHERE
# MAGIC         tpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND tpep_pickup_datetime < '2025-01-01'
# MAGIC         AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND tpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS rows_in_date_range,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes <= 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_or_negative_duration,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 180
# MAGIC         THEN 1 ELSE 0 END) AS over_3_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 360
# MAGIC         THEN 1 ELSE 0 END) AS over_6_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 720
# MAGIC         THEN 1 ELSE 0 END) AS over_12_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 1440
# MAGIC         THEN 1 ELSE 0 END) AS over_24_hours
# MAGIC
# MAGIC FROM yellow_duration;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_duration AS (
# MAGIC     SELECT
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0 AS duration_minutes
# MAGIC     FROM green_raw
# MAGIC     WHERE
# MAGIC         lpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND lpep_pickup_datetime < '2025-01-01'
# MAGIC         AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND lpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS rows_in_date_range,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes <= 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_or_negative_duration,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 180
# MAGIC         THEN 1 ELSE 0 END) AS over_3_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 360
# MAGIC         THEN 1 ELSE 0 END) AS over_6_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 720
# MAGIC         THEN 1 ELSE 0 END) AS over_12_hours,
# MAGIC
# MAGIC     SUM(CASE WHEN duration_minutes > 1440
# MAGIC         THEN 1 ELSE 0 END) AS over_24_hours
# MAGIC
# MAGIC FROM green_duration;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_distance,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance = 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_distance,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 50
# MAGIC         THEN 1 ELSE 0 END) AS over_50_miles,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 100
# MAGIC         THEN 1 ELSE 0 END) AS over_100_miles,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 200
# MAGIC         THEN 1 ELSE 0 END) AS over_200_miles
# MAGIC
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_distance,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance = 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_distance,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 50
# MAGIC         THEN 1 ELSE 0 END) AS over_50_miles,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 100
# MAGIC         THEN 1 ELSE 0 END) AS over_100_miles,
# MAGIC
# MAGIC     SUM(CASE WHEN trip_distance > 200
# MAGIC         THEN 1 ELSE 0 END) AS over_200_miles
# MAGIC
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_total_amount,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_total_amount,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count = 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN PULocationID IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_pickup_location,
# MAGIC
# MAGIC     SUM(CASE WHEN DOLocationID IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_dropoff_location
# MAGIC
# MAGIC FROM yellow_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_total_amount,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_total_amount,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count < 0
# MAGIC         THEN 1 ELSE 0 END) AS negative_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN passenger_count = 0
# MAGIC         THEN 1 ELSE 0 END) AS zero_passenger_count,
# MAGIC
# MAGIC     SUM(CASE WHEN PULocationID IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_pickup_location,
# MAGIC
# MAGIC     SUM(CASE WHEN DOLocationID IS NULL
# MAGIC         THEN 1 ELSE 0 END) AS null_dropoff_location
# MAGIC
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW taxi_zone_lookup
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC     path '/Volumes/workspace/bde/assignment2/taxi_zone_lookup.csv',
# MAGIC     header 'true',
# MAGIC     inferSchema 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM taxi_zone_lookup
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_duration AS (
# MAGIC     SELECT
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_minutes
# MAGIC     FROM yellow_raw
# MAGIC     WHERE
# MAGIC         tpep_dropoff_datetime > tpep_pickup_datetime
# MAGIC         AND tpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND tpep_pickup_datetime < '2025-01-01'
# MAGIC         AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND tpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN duration_minutes > 60  AND duration_minutes <= 180  THEN 1 ELSE 0 END) AS min_60_180,
# MAGIC     SUM(CASE WHEN duration_minutes > 180 AND duration_minutes <= 360  THEN 1 ELSE 0 END) AS min_180_360,
# MAGIC     SUM(CASE WHEN duration_minutes > 360 AND duration_minutes <= 720  THEN 1 ELSE 0 END) AS min_360_720,
# MAGIC     SUM(CASE WHEN duration_minutes > 720 AND duration_minutes <= 1380 THEN 1 ELSE 0 END) AS min_720_1380,
# MAGIC     SUM(CASE WHEN duration_minutes > 1380 AND duration_minutes <= 1440 THEN 1 ELSE 0 END) AS min_1380_1440,
# MAGIC     SUM(CASE WHEN duration_minutes > 1440 THEN 1 ELSE 0 END) AS over_1440
# MAGIC FROM yellow_duration;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_duration AS (
# MAGIC     SELECT
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0 AS duration_minutes
# MAGIC     FROM green_raw
# MAGIC     WHERE
# MAGIC         lpep_dropoff_datetime > lpep_pickup_datetime
# MAGIC         AND lpep_pickup_datetime >= '2014-01-01'
# MAGIC         AND lpep_pickup_datetime < '2025-01-01'
# MAGIC         AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC         AND lpep_dropoff_datetime < '2025-01-01'
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN duration_minutes > 60  AND duration_minutes <= 180  THEN 1 ELSE 0 END) AS min_60_180,
# MAGIC     SUM(CASE WHEN duration_minutes > 180 AND duration_minutes <= 360  THEN 1 ELSE 0 END) AS min_180_360,
# MAGIC     SUM(CASE WHEN duration_minutes > 360 AND duration_minutes <= 720  THEN 1 ELSE 0 END) AS min_360_720,
# MAGIC     SUM(CASE WHEN duration_minutes > 720 AND duration_minutes <= 1380 THEN 1 ELSE 0 END) AS min_720_1380,
# MAGIC     SUM(CASE WHEN duration_minutes > 1380 AND duration_minutes <= 1440 THEN 1 ELSE 0 END) AS min_1380_1440,
# MAGIC     SUM(CASE WHEN duration_minutes > 1440 THEN 1 ELSE 0 END) AS over_1440
# MAGIC FROM green_duration;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_checks AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_minutes,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC              unix_timestamp(tpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC
# MAGIC     FROM yellow_raw
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN tpep_dropoff_datetime < tpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS finish_before_start,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN tpep_dropoff_datetime = tpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS zero_duration,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN tpep_pickup_datetime < '2014-01-01'
# MAGIC           OR tpep_pickup_datetime >= '2025-01-01'
# MAGIC           OR tpep_dropoff_datetime < '2014-01-01'
# MAGIC           OR tpep_dropoff_datetime >= '2025-01-01'
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS outside_date_range,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN speed_kmh < 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS negative_speed,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN trip_distance < 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS negative_distance
# MAGIC
# MAGIC FROM yellow_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_checks AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0 AS duration_minutes,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC              unix_timestamp(lpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC
# MAGIC     FROM green_raw
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN lpep_dropoff_datetime < lpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS finish_before_start,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN lpep_dropoff_datetime = lpep_pickup_datetime
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS zero_duration,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN lpep_pickup_datetime < '2014-01-01'
# MAGIC           OR lpep_pickup_datetime >= '2025-01-01'
# MAGIC           OR lpep_dropoff_datetime < '2014-01-01'
# MAGIC           OR lpep_dropoff_datetime >= '2025-01-01'
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS outside_date_range,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN speed_kmh < 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS negative_speed,
# MAGIC
# MAGIC     SUM(CASE
# MAGIC         WHEN trip_distance < 0
# MAGIC         THEN 1 ELSE 0
# MAGIC     END) AS negative_distance
# MAGIC
# MAGIC FROM green_checks;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH yellow_cleaning_check AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_minutes,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC              unix_timestamp(tpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC     FROM yellow_raw
# MAGIC ),
# MAGIC
# MAGIC flagged AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 1
# MAGIC             WHEN tpep_pickup_datetime < '2014-01-01'
# MAGIC               OR tpep_pickup_datetime >= '2025-01-01'
# MAGIC               OR tpep_dropoff_datetime < '2014-01-01'
# MAGIC               OR tpep_dropoff_datetime >= '2025-01-01' THEN 1
# MAGIC             WHEN trip_distance < 0 THEN 1
# MAGIC             WHEN speed_kmh < 0 THEN 1
# MAGIC             WHEN speed_kmh > 120 THEN 1
# MAGIC             WHEN duration_minutes > 360 THEN 1
# MAGIC             WHEN trip_distance > 100 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS remove_flag
# MAGIC     FROM yellow_cleaning_check
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(remove_flag) AS rows_to_remove,
# MAGIC     COUNT(*) - SUM(remove_flag) AS rows_remaining,
# MAGIC     ROUND(SUM(remove_flag) * 100.0 / COUNT(*), 4) AS percent_removed
# MAGIC FROM flagged;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH green_cleaning_check AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0 AS duration_minutes,
# MAGIC
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC              unix_timestamp(lpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC     FROM green_raw
# MAGIC ),
# MAGIC
# MAGIC flagged AS (
# MAGIC     SELECT *,
# MAGIC         CASE
# MAGIC             WHEN lpep_dropoff_datetime <= lpep_pickup_datetime THEN 1
# MAGIC
# MAGIC             WHEN lpep_pickup_datetime < '2014-01-01'
# MAGIC               OR lpep_pickup_datetime >= '2025-01-01'
# MAGIC               OR lpep_dropoff_datetime < '2014-01-01'
# MAGIC               OR lpep_dropoff_datetime >= '2025-01-01' THEN 1
# MAGIC
# MAGIC             WHEN trip_distance < 0 THEN 1
# MAGIC             WHEN speed_kmh < 0 THEN 1
# MAGIC             WHEN speed_kmh > 120 THEN 1
# MAGIC             WHEN duration_minutes > 360 THEN 1
# MAGIC             WHEN trip_distance > 100 THEN 1
# MAGIC
# MAGIC             ELSE 0
# MAGIC         END AS remove_flag
# MAGIC     FROM green_cleaning_check
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     SUM(remove_flag) AS rows_to_remove,
# MAGIC     COUNT(*) - SUM(remove_flag) AS rows_remaining,
# MAGIC     ROUND(SUM(remove_flag) * 100.0 / COUNT(*), 4) AS percent_removed
# MAGIC FROM flagged;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'Yellow' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     ROUND(MIN(total_amount), 2) AS min_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.50), 2) AS median_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.90), 2) AS p90_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.95), 2) AS p95_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.99), 2) AS p99_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.999), 2) AS p999_amount,
# MAGIC     ROUND(MAX(total_amount), 2) AS max_amount
# MAGIC FROM yellow_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Green' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     ROUND(MIN(total_amount), 2) AS min_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.50), 2) AS median_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.90), 2) AS p90_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.95), 2) AS p95_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.99), 2) AS p99_amount,
# MAGIC     ROUND(percentile_approx(total_amount, 0.999), 2) AS p999_amount,
# MAGIC     ROUND(MAX(total_amount), 2) AS max_amount
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'Yellow' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_count,
# MAGIC     SUM(CASE WHEN total_amount > 500 THEN 1 ELSE 0 END) AS over_500,
# MAGIC     SUM(CASE WHEN total_amount > 1000 THEN 1 ELSE 0 END) AS over_1000,
# MAGIC     SUM(CASE WHEN total_amount > 10000 THEN 1 ELSE 0 END) AS over_10000
# MAGIC
# MAGIC FROM yellow_raw
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Green' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN total_amount > 500 THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN total_amount > 1000 THEN 1 ELSE 0 END),
# MAGIC     SUM(CASE WHEN total_amount > 10000 THEN 1 ELSE 0 END)
# MAGIC
# MAGIC FROM green_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     total_amount,
# MAGIC     trip_distance,
# MAGIC     ROUND(trip_distance * 1.609344, 2) AS distance_km,
# MAGIC
# MAGIC     ROUND(
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0,
# MAGIC         2
# MAGIC     ) AS duration_minutes,
# MAGIC
# MAGIC     ROUND(
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC              unix_timestamp(tpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS speed_kmh,
# MAGIC
# MAGIC     fare_amount,
# MAGIC     tip_amount
# MAGIC
# MAGIC FROM yellow_raw
# MAGIC
# MAGIC ORDER BY total_amount DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     total_amount,
# MAGIC     trip_distance,
# MAGIC     ROUND(trip_distance * 1.609344, 2) AS distance_km,
# MAGIC
# MAGIC     ROUND(
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0,
# MAGIC         2
# MAGIC     ) AS duration_minutes,
# MAGIC
# MAGIC     ROUND(
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC              unix_timestamp(lpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS speed_kmh,
# MAGIC
# MAGIC     fare_amount,
# MAGIC     tip_amount
# MAGIC
# MAGIC FROM green_raw
# MAGIC
# MAGIC ORDER BY total_amount DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Exploration Findings:
# MAGIC
# MAGIC Before data cleaning, the Yellow and Green Taxi datasets were explored using SQL to identify unrealistic trips based on datetime, duration, distance, and speed.
# MAGIC
# MAGIC * **Date and time:** Some trips had drop-off times earlier than their pickup times (**Yellow: 94,024; Green: 1,042**). Records outside the expected **2014–2024** period were also identified (**Yellow: 3,337; Green: 816**). Zero-duration trips were also observed (**Yellow: 1,014,962; Green: 67,276**).
# MAGIC
# MAGIC * **Trip duration:** Negative and extremely large duration values were identified. While most trips were concentrated within reasonable durations, an unusual concentration of trips between **23 and 24 hours** was observed in both datasets (**Yellow: 1,078,354; Green: 271,584**). This suggests that some extremely long durations may be caused by timestamp or data-recording anomalies.
# MAGIC
# MAGIC * **Trip distance:** Negative trip distances were identified (**Yellow: 11,444; Green: 19,527**), together with extremely large and unrealistic distance values. The **99.9th percentile was approximately 26 miles** for both datasets, while the maximum values were substantially higher. Zero-distance trips were also observed but were not automatically considered invalid.
# MAGIC
# MAGIC * **Trip speed:** Negative and physically unrealistic average speeds were observed. The **99th percentile of speed was 56.80 km/h for Yellow taxis and 54.44 km/h for Green taxis**, while extreme maximum values reached millions or even billions of km/h. The general NYC speed limit is **25 mph (~40 km/h)**, while the maximum speed limit in New York State is **65 mph (~105 km/h)**. Therefore, extremely high calculated speeds were considered indicative of data anomalies.
# MAGIC
# MAGIC * **Total amount:** The total_amount distribution was also examined for extreme monetary anomalies. After the initial trip-validity rules were applied, the 99.9th percentile was approximately $122.64 for Yellow taxis and $100 for Green taxis, while extreme maximum values of approximately $10 million and $990,000 remained. Individual inspection showed that some short-distance and short-duration trips had implausibly large total amounts. Negative total_amount values were also observed but were not automatically considered invalid.
# MAGIC
# MAGIC
# MAGIC **Overall finding:** The exploration identified invalid datetime sequences, dates outside the dataset period, negative distance/speed values, extremely high speeds, excessively long durations, extremely large distances, and extreme positive total_amount values as the main data-quality issues. These findings were subsequently used to define the filtering rules applied during the data-cleaning stage.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Data Cleaning 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE yellow_clean AS
# MAGIC
# MAGIC WITH yellow_prepared AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC
# MAGIC         -- Trip duration in minutes
# MAGIC         (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC          unix_timestamp(tpep_pickup_datetime)) / 60.0
# MAGIC             AS duration_minutes,
# MAGIC
# MAGIC         -- Average trip speed in km/h
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(tpep_dropoff_datetime) -
# MAGIC              unix_timestamp(tpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC
# MAGIC     FROM yellow_raw
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM yellow_prepared
# MAGIC
# MAGIC WHERE
# MAGIC     -- (a) Valid trip time order / (e) positive duration
# MAGIC     tpep_dropoff_datetime > tpep_pickup_datetime
# MAGIC
# MAGIC     -- (b) Dataset period: 2014-2024
# MAGIC     AND tpep_pickup_datetime >= '2014-01-01'
# MAGIC     AND tpep_pickup_datetime <  '2025-01-01'
# MAGIC     AND tpep_dropoff_datetime >= '2014-01-01'
# MAGIC     AND tpep_dropoff_datetime <  '2025-01-01'
# MAGIC
# MAGIC     -- (c) No negative distance / speed
# MAGIC     AND trip_distance >= 0
# MAGIC     AND speed_kmh >= 0
# MAGIC
# MAGIC     -- (d) Remove unrealistically high average speeds
# MAGIC     AND speed_kmh <= 120
# MAGIC
# MAGIC     -- (e) Remove unrealistically long trips
# MAGIC     AND duration_minutes <= 360
# MAGIC
# MAGIC     -- (f) Remove unrealistically long distances
# MAGIC     AND trip_distance <= 100
# MAGIC
# MAGIC     -- (g) Remove extreme positive total amounts that are inconsistent
# MAGIC     -- with the physical characteristics of the trips (e.g Max:10,000,015 this is unrelastic value bcs I checked for km, distance and all columns.)
# MAGIC     AND total_amount <= 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS yellow_clean_rows
# MAGIC FROM yellow_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE green_clean AS
# MAGIC
# MAGIC WITH green_prepared AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC
# MAGIC         -- Trip duration in minutes
# MAGIC         (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC          unix_timestamp(lpep_pickup_datetime)) / 60.0
# MAGIC             AS duration_minutes,
# MAGIC
# MAGIC         -- Average trip speed in km/h
# MAGIC         (trip_distance * 1.609344) /
# MAGIC         NULLIF(
# MAGIC             (unix_timestamp(lpep_dropoff_datetime) -
# MAGIC              unix_timestamp(lpep_pickup_datetime)) / 3600.0,
# MAGIC             0
# MAGIC         ) AS speed_kmh
# MAGIC
# MAGIC     FROM green_raw
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM green_prepared
# MAGIC
# MAGIC WHERE
# MAGIC     -- (a) Valid trip time order / (e) positive duration
# MAGIC     lpep_dropoff_datetime > lpep_pickup_datetime
# MAGIC
# MAGIC     -- (b) Dataset period: 2014-2024
# MAGIC     AND lpep_pickup_datetime >= '2014-01-01'
# MAGIC     AND lpep_pickup_datetime <  '2025-01-01'
# MAGIC     AND lpep_dropoff_datetime >= '2014-01-01'
# MAGIC     AND lpep_dropoff_datetime <  '2025-01-01'
# MAGIC
# MAGIC     -- (c) No negative distance / speed
# MAGIC     AND trip_distance >= 0
# MAGIC     AND speed_kmh >= 0
# MAGIC
# MAGIC     -- (d) Remove unrealistically high average speeds
# MAGIC     AND speed_kmh <= 120
# MAGIC
# MAGIC     -- (e) Remove unrealistically long trips
# MAGIC     AND duration_minutes <= 360
# MAGIC
# MAGIC     -- (f) Remove unrealistically long distances
# MAGIC     AND trip_distance <= 100
# MAGIC
# MAGIC     -- (g) Remove extreme positive total amounts that are inconsistent
# MAGIC     -- with the physical characteristics of the trips(e.g 989,970.39 → 0.29 km, 2.53 dk, fare sadece 3.50, 88,064.93 → 1.90 km, 2 dk, fare 4.50, 51,192 → 3.54 km, 14.78 dk, fare 11)
# MAGIC     AND total_amount <= 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS green_clean_rows
# MAGIC FROM green_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleaning Decision: 
# MAGIC
# MAGIC - Based on the exploration findings, unrealistic trips were removed using SQL. Trips were excluded if they had invalid pickup/drop-off times, dates outside the 2014–2024 period, negative distance or speed, average speeds above 120 km/h, durations longer than 360 minutes (6 hours), or distances greater than 100 miles.
# MAGIC - The 120 km/h speed threshold was selected conservatively considering the general NYC speed limit of 25 mph (40 km/h) and the New York State maximum speed limit of 65 mph (105 km/h). The duration and distance thresholds were selected based on the observed distributions and extreme outliers while retaining legitimate long-distance and 60+ minute trips required for later analysis.
# MAGIC - Zero-distance trips, missing/zero passenger counts, and negative total_amount values were not automatically removed because these values alone do not necessarily indicate an unrealistic trip.
# MAGIC - Additional Cleaning Logic – Question 1(g):
# MAGIC - After applying the previous cleaning rules, the distribution of total_amount was further examined. The distributions were highly concentrated at much lower values, with the 99.9th percentile at approximately $122.64 for Yellow taxis and $100 for Green taxis, while extreme maximum values of approximately $10 million and $990,000 remained.
# MAGIC - Records with total_amount > $10,000 were investigated individually and were found to be inconsistent with the physical characteristics of the corresponding trips. For example, some trips of only a few kilometres and a few minutes had recorded total amounts ranging from tens of thousands to several million dollars. In several cases, these extreme values were associated with anomalous monetary components such as fare_amount or tip_amount. Therefore, a conservative threshold of $10,000 was used to remove only clearly extreme positive monetary anomalies.
# MAGIC - Negative total_amount records were retained because inspection showed that negative totals were commonly accompanied by negative monetary components, particularly fare_amount, suggesting that some may represent refunds, reversals, or transaction corrections. Since their validity could not be conclusively rejected, removing them could discard legitimate financial adjustments.
# MAGIC - This additional rule removed 100 Yellow and 3 Green taxi records. After all cleaning rules were applied, 904,655,251 Yellow and 82,841,606 Green taxi trips remained. The overall proportion of removed observations remained approximately 0.40%, well below the assignment's maximum 10% removal limit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Count the total number of rows for both green and yellow taxis.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     'Yellow' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows
# MAGIC FROM yellow_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Green' AS taxi_colour,
# MAGIC     COUNT(*) AS total_rows
# MAGIC FROM green_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Combine the yellow and green taxi dataset together (their schema are not exactly the same).

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE taxi_combined AS
# MAGIC
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     tpep_pickup_datetime AS pickup_datetime,
# MAGIC     tpep_dropoff_datetime AS dropoff_datetime,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     RatecodeID,
# MAGIC     store_and_fwd_flag,
# MAGIC     PULocationID,
# MAGIC     DOLocationID,
# MAGIC     CAST(payment_type AS DOUBLE) AS payment_type,
# MAGIC     fare_amount,
# MAGIC     extra,
# MAGIC     mta_tax,
# MAGIC     tip_amount,
# MAGIC     tolls_amount,
# MAGIC     improvement_surcharge,
# MAGIC     total_amount,
# MAGIC     congestion_surcharge,
# MAGIC     airport_fee,
# MAGIC     CAST(NULL AS DOUBLE) AS ehail_fee,
# MAGIC     CAST(NULL AS DOUBLE) AS trip_type,
# MAGIC     duration_minutes,
# MAGIC     speed_kmh,
# MAGIC     'Yellow' AS taxi_colour
# MAGIC
# MAGIC FROM yellow_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     VendorID,
# MAGIC     lpep_pickup_datetime AS pickup_datetime,
# MAGIC     lpep_dropoff_datetime AS dropoff_datetime,
# MAGIC     passenger_count,
# MAGIC     trip_distance,
# MAGIC     RatecodeID,
# MAGIC     store_and_fwd_flag,
# MAGIC     PULocationID,
# MAGIC     DOLocationID,
# MAGIC     CAST(payment_type AS DOUBLE) AS payment_type,
# MAGIC     fare_amount,
# MAGIC     extra,
# MAGIC     mta_tax,
# MAGIC     tip_amount,
# MAGIC     tolls_amount,
# MAGIC     improvement_surcharge,
# MAGIC     total_amount,
# MAGIC     congestion_surcharge,
# MAGIC     CAST(NULL AS DOUBLE) AS airport_fee,
# MAGIC     ehail_fee,
# MAGIC     trip_type,
# MAGIC     duration_minutes,
# MAGIC     speed_kmh,
# MAGIC     'Green' AS taxi_colour
# MAGIC
# MAGIC FROM green_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS combined_rows
# MAGIC FROM taxi_combined;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC After cleaning, the Yellow and Green taxi datasets contained 904,655,351 and 82,841,609 trips, respectively.
# MAGIC
# MAGIC Before combining the datasets, their schemas were aligned because the Yellow and Green datasets use slightly different column names and structures. Pickup and drop-off datetime columns were renamed to common names, data types such as payment_type were standardised, and columns available in only one dataset were represented as NULL for the other dataset. A taxi_colour column was also added to preserve whether each trip originated from the Yellow or Green dataset.
# MAGIC
# MAGIC The two cleaned datasets were then combined vertically using UNION ALL, retaining all trip records without removing duplicates. The resulting taxi_combined table contained 987,496,960 trips, confirming that no records were lost during the combination process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6	Combine the new dataframe/table with the location data (there are two locations in each trip, pick up location and drop off location)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE taxi_with_locations AS
# MAGIC
# MAGIC SELECT
# MAGIC     t.*,
# MAGIC
# MAGIC     pu.Borough AS pickup_borough,
# MAGIC     pu.Zone AS pickup_zone,
# MAGIC     pu.service_zone AS pickup_service_zone,
# MAGIC
# MAGIC     do.Borough AS dropoff_borough,
# MAGIC     do.Zone AS dropoff_zone,
# MAGIC     do.service_zone AS dropoff_service_zone
# MAGIC
# MAGIC FROM taxi_combined t
# MAGIC
# MAGIC LEFT JOIN taxi_zone_lookup pu
# MAGIC     ON t.PULocationID = pu.LocationID
# MAGIC
# MAGIC LEFT JOIN taxi_zone_lookup do
# MAGIC     ON t.DOLocationID = do.LocationID;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS rows_after_location_join
# MAGIC FROM taxi_with_locations;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(CASE WHEN pickup_borough IS NULL THEN 1 ELSE 0 END) AS missing_pickup_location,
# MAGIC     SUM(CASE WHEN dropoff_borough IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_location
# MAGIC FROM taxi_with_locations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Save the data set 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE taxi_final AS
# MAGIC SELECT *
# MAGIC FROM taxi_with_locations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8. Final Row Count

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS final_total_rows
# MAGIC FROM taxi_final;