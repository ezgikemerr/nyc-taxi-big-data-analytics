# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Part 2: Business Questions
# MAGIC
# MAGIC
# MAGIC The analyses in this section use the final cleaned and location-enriched
# MAGIC `taxi_final` table created in Part 1.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.default.taxi_final
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     COUNT(DISTINCT taxi_colour) AS taxi_colours
# MAGIC FROM workspace.default.taxi_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Business Question 1:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Prepare the fields required for the monthly analysis
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         DATE_FORMAT(pickup_datetime, 'yyyy-MM') AS year_month,
# MAGIC         DATE_FORMAT(pickup_datetime, 'EEEE') AS weekday,
# MAGIC         HOUR(pickup_datetime) AS pickup_hour,
# MAGIC         passenger_count,
# MAGIC         total_amount
# MAGIC     FROM workspace.default.taxi_final
# MAGIC ),
# MAGIC
# MAGIC -- Calculate monthly trip, passenger and payment statistics
# MAGIC monthly_summary AS (
# MAGIC     SELECT
# MAGIC         year_month,
# MAGIC         COUNT(*) AS total_trips,
# MAGIC         AVG(passenger_count) AS avg_passengers,
# MAGIC         AVG(total_amount) AS avg_amount_per_trip,
# MAGIC
# MAGIC         -- Only trips with a positive passenger count are used
# MAGIC         -- to avoid invalid per-passenger calculations
# MAGIC         SUM(
# MAGIC             CASE 
# MAGIC                 WHEN passenger_count > 0 THEN total_amount
# MAGIC             END
# MAGIC         ) /
# MAGIC         SUM(
# MAGIC             CASE 
# MAGIC                 WHEN passenger_count > 0 THEN passenger_count
# MAGIC             END
# MAGIC         ) AS avg_amount_per_passenger
# MAGIC
# MAGIC     FROM base
# MAGIC     GROUP BY year_month
# MAGIC ),
# MAGIC
# MAGIC -- Rank weekdays by the number of trips within each month
# MAGIC weekday_counts AS (
# MAGIC     SELECT
# MAGIC         year_month,
# MAGIC         weekday,
# MAGIC         COUNT(*) AS trip_count,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY year_month
# MAGIC             ORDER BY COUNT(*) DESC, weekday
# MAGIC         ) AS rn
# MAGIC     FROM base
# MAGIC     GROUP BY year_month, weekday
# MAGIC ),
# MAGIC
# MAGIC -- Rank pickup hours by the number of trips within each month
# MAGIC hour_counts AS (
# MAGIC     SELECT
# MAGIC         year_month,
# MAGIC         pickup_hour,
# MAGIC         COUNT(*) AS trip_count,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY year_month
# MAGIC             ORDER BY COUNT(*) DESC, pickup_hour
# MAGIC         ) AS rn
# MAGIC     FROM base
# MAGIC     GROUP BY year_month, pickup_hour
# MAGIC )
# MAGIC
# MAGIC -- Combine all Q1 metrics into a single output
# MAGIC SELECT
# MAGIC     m.year_month,
# MAGIC     m.total_trips,
# MAGIC     w.weekday AS busiest_weekday,
# MAGIC     h.pickup_hour AS busiest_hour,
# MAGIC     ROUND(m.avg_passengers, 2) AS avg_passengers,
# MAGIC     ROUND(m.avg_amount_per_trip, 2) AS avg_amount_per_trip,
# MAGIC     ROUND(m.avg_amount_per_passenger, 2) AS avg_amount_per_passenger
# MAGIC
# MAGIC FROM monthly_summary m
# MAGIC
# MAGIC LEFT JOIN weekday_counts w
# MAGIC     ON m.year_month = w.year_month
# MAGIC     AND w.rn = 1
# MAGIC
# MAGIC LEFT JOIN hour_counts h
# MAGIC     ON m.year_month = h.year_month
# MAGIC     AND h.rn = 1
# MAGIC
# MAGIC ORDER BY m.year_month;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q1: The aim of this business analysis was to examine monthly taxi activity by analysing trip demand, peak weekdays and hours, average passenger numbers, and average payment patterns per trip and per passenger.
# MAGIC
# MAGIC The monthly analysis shows clear changes in taxi demand, passenger counts, and payment levels between 2014 and 2024. 
# MAGIC
# MAGIC This question is about a monthly overview of taxi activity. For each year and month, the total number of trips, busiest weekday, busiest hour, average number of passengers, average amount paid per trip, and average amount paid per passenger were calculated.
# MAGIC
# MAGIC - **Trip volume:** Monthly trip volumes were highest in the earlier years of the dataset, often exceeding 14 million trips per month in 2014–2015. Trip volumes declined over time, with a particularly sharp decrease during 2020. For example, trips fell from 6.67 million in February 2020 to only 0.27 million in April 2020, followed by a gradual recovery in subsequent years.
# MAGIC
# MAGIC - **Busiest weekday and hour:** The busiest weekday varies across months. The busiest pickup hour was generally around **18:00–19:00**, although during the low-demand period in 2020 and early 2021 it shifted earlier to approximately **14:00–15:00**.
# MAGIC
# MAGIC - **Average passengers:** Average passenger numbers generally declined over the study period, from approximately **1.7 passengers per trip in 2014** to around **1.3–1.4 passengers per trip in 2024**.
# MAGIC
# MAGIC - **Average payments:** The average amount paid per trip increased substantially over time. Monthly averages were generally around **14–16 per trip in 2014**, compared with approximately **27–29 per trip during 2023–2024**. A similar upward pattern is visible in the average amount paid per passenger.
# MAGIC
# MAGIC - **Data coverage note:** March 2017 shows an unusually low trip count of **1,148,155**. A check of the raw data confirmed that no Yellow Taxi records were available for March 2017, meaning that this month's result represents Green Taxi trips only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Business Question 2:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     taxi_colour,
# MAGIC
# MAGIC     -- (a) Trip duration statistics in minutes
# MAGIC     ROUND(AVG(duration_minutes), 2) AS avg_duration_min,
# MAGIC     ROUND(PERCENTILE_APPROX(duration_minutes, 0.5), 2) AS median_duration_min,
# MAGIC     ROUND(MIN(duration_minutes), 2) AS min_duration_min,
# MAGIC     ROUND(MAX(duration_minutes), 2) AS max_duration_min,
# MAGIC
# MAGIC     -- (b) Trip distance statistics converted from miles to kilometres
# MAGIC     ROUND(AVG(trip_distance * 1.609344), 2) AS avg_distance_km,
# MAGIC     ROUND(PERCENTILE_APPROX(trip_distance * 1.609344, 0.5), 2) AS median_distance_km,
# MAGIC     ROUND(MIN(trip_distance * 1.609344), 2) AS min_distance_km,
# MAGIC     ROUND(MAX(trip_distance * 1.609344), 2) AS max_distance_km,
# MAGIC
# MAGIC     -- (c) Trip speed statistics in km/h
# MAGIC     ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
# MAGIC     ROUND(PERCENTILE_APPROX(speed_kmh, 0.5), 2) AS median_speed_kmh,
# MAGIC     ROUND(MIN(speed_kmh), 2) AS min_speed_kmh,
# MAGIC     ROUND(MAX(speed_kmh), 2) AS max_speed_kmh
# MAGIC
# MAGIC FROM workspace.default.taxi_final
# MAGIC
# MAGIC GROUP BY taxi_colour
# MAGIC ORDER BY taxi_colour;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q2: The aim of Question 2 was to compare Yellow and Green taxi trip characteristics by analysing the average, median, minimum, and maximum trip duration, distance, and speed.
# MAGIC
# MAGIC The results compare trip duration, distance, and speed statistics for Green and Yellow taxis.
# MAGIC
# MAGIC This question is about: trip characteristics of Green and Yellow taxis. For each taxi colour, the average, median, minimum, and maximum trip duration, trip distance in kilometres, and speed in kilometres per hour were calculated.
# MAGIC
# MAGIC - **Trip duration:** Yellow taxis had slightly longer trips, with an average duration of **14.35 minutes** and a median of **11.23 minutes**, compared with **13.73 minutes** and **10.52 minutes** for Green taxis. Both taxi colours had a minimum duration of **0.02 minutes** and a maximum of **360 minutes**, with the maximum reflecting the six-hour cleaning threshold applied in Part 1.
# MAGIC
# MAGIC - **Trip distance:** Average trip distances were very similar, at **4.81 km for Green** and **4.87 km for Yellow**. However, Green taxis had a higher median distance (**3.07 km**) than Yellow taxis (**2.74 km**). Both taxi colours included zero-distance trips, which were retained during cleaning, while maximum distances were approximately **160.9 km**, consistent with the 100-mile upper limit applied in Part 1.
# MAGIC
# MAGIC - **Trip speed:** Green taxis showed higher typical speeds, with an average of **20.19 km/h** and a median of **18.45 km/h**, compared with **18.77 km/h** and **16.52 km/h** for Yellow taxis. Minimum speeds were **0 km/h**, reflecting the retained zero-distance trips, while maximum speeds were approximately **120 km/h**, consistent with the cleaning threshold.
# MAGIC
# MAGIC Overall, the two taxi colours had relatively similar trip characteristics, although Yellow taxis tended to have slightly longer trips while Green taxis had higher median distances and higher typical speeds. The observed maximum values are also consistent with the cleaning thresholds established in Part 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Business Question 3:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     taxi_colour,
# MAGIC
# MAGIC     -- Use borough names rather than location IDs
# MAGIC     pickup_borough,
# MAGIC     dropoff_borough,
# MAGIC
# MAGIC     -- Time dimensions
# MAGIC     DATE_FORMAT(pickup_datetime, 'yyyy-MM') AS year_month,
# MAGIC     DATE_FORMAT(pickup_datetime, 'EEEE') AS weekday,
# MAGIC     HOUR(pickup_datetime) AS pickup_hour,
# MAGIC
# MAGIC     -- (a) Total number of trips
# MAGIC     COUNT(*) AS total_trips,
# MAGIC
# MAGIC     -- (b) Average trip distance in kilometres
# MAGIC     ROUND(AVG(trip_distance * 1.609344), 2) AS avg_distance_km,
# MAGIC
# MAGIC     -- (c) Average amount paid per trip
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_amount_per_trip,
# MAGIC
# MAGIC     -- (d) Total amount paid
# MAGIC     ROUND(SUM(total_amount), 2) AS total_amount_paid
# MAGIC
# MAGIC FROM workspace.default.taxi_final
# MAGIC
# MAGIC GROUP BY
# MAGIC     taxi_colour,
# MAGIC     pickup_borough,
# MAGIC     dropoff_borough,
# MAGIC     DATE_FORMAT(pickup_datetime, 'yyyy-MM'),
# MAGIC     DATE_FORMAT(pickup_datetime, 'EEEE'),
# MAGIC     HOUR(pickup_datetime)
# MAGIC
# MAGIC ORDER BY
# MAGIC     year_month,
# MAGIC     taxi_colour,
# MAGIC     pickup_borough,
# MAGIC     dropoff_borough,
# MAGIC     weekday,
# MAGIC     pickup_hour;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ---For Part 3: Machine Learning part. Bcs need to according to year. Save the dataset in the catalog.
# MAGIC
# MAGIC CREATE OR REPLACE TABLE workspace.default.q3_business_analysis AS
# MAGIC
# MAGIC SELECT
# MAGIC     taxi_colour,
# MAGIC
# MAGIC     -- Use borough names rather than location IDs
# MAGIC     pickup_borough,
# MAGIC     dropoff_borough,
# MAGIC
# MAGIC     -- Time dimensions
# MAGIC     DATE_FORMAT(pickup_datetime, 'yyyy-MM') AS year_month,
# MAGIC     DATE_FORMAT(pickup_datetime, 'EEEE') AS weekday,
# MAGIC     HOUR(pickup_datetime) AS pickup_hour,
# MAGIC
# MAGIC     -- (a) Total number of trips
# MAGIC     COUNT(*) AS total_trips,
# MAGIC
# MAGIC     -- (b) Average trip distance in kilometres
# MAGIC     ROUND(AVG(trip_distance * 1.609344), 2) AS avg_distance_km,
# MAGIC
# MAGIC     -- (c) Average amount paid per trip
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_amount_per_trip,
# MAGIC
# MAGIC     -- (d) Total amount paid
# MAGIC     ROUND(SUM(total_amount), 2) AS total_amount_paid
# MAGIC
# MAGIC FROM workspace.default.taxi_final
# MAGIC
# MAGIC GROUP BY
# MAGIC     taxi_colour,
# MAGIC     pickup_borough,
# MAGIC     dropoff_borough,
# MAGIC     DATE_FORMAT(pickup_datetime, 'yyyy-MM'),
# MAGIC     DATE_FORMAT(pickup_datetime, 'EEEE'),
# MAGIC     HOUR(pickup_datetime);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q3: The aim of Question 3 was to analyse taxi activity and revenue patterns across different taxi colours, pickup and drop-off borough pairs, months, weekdays, and hours.
# MAGIC
# MAGIC The table provides a detailed breakdown of taxi activity by taxi colour, pickup and drop-off borough, month, weekday, and hour. For each combination, it reports the total number of trips, average trip distance, average amount paid per trip, and total amount paid. The month was represented as year-month to keep different years separate and to allow the aggregated results to be used as the historical baseline in Part 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Business Question 4:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH route_revenue AS (
# MAGIC
# MAGIC     -- Calculate total 2023 revenue for each directional borough pair
# MAGIC     SELECT
# MAGIC         pickup_borough,
# MAGIC         dropoff_borough,
# MAGIC         SUM(total_amount) AS route_revenue
# MAGIC     FROM workspace.default.taxi_final
# MAGIC     WHERE pickup_datetime >= '2023-01-01'
# MAGIC       AND pickup_datetime <  '2024-01-01'
# MAGIC     GROUP BY
# MAGIC         pickup_borough,
# MAGIC         dropoff_borough
# MAGIC ),
# MAGIC
# MAGIC ranked_routes AS (
# MAGIC
# MAGIC     -- Rank borough pairs by total revenue
# MAGIC     SELECT
# MAGIC         pickup_borough,
# MAGIC         dropoff_borough,
# MAGIC         route_revenue,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY route_revenue DESC
# MAGIC         ) AS revenue_rank
# MAGIC     FROM route_revenue
# MAGIC ),
# MAGIC
# MAGIC top_10 AS (
# MAGIC
# MAGIC     -- Keep only the Top 10 revenue-generating borough pairs
# MAGIC     SELECT *
# MAGIC     FROM ranked_routes
# MAGIC     WHERE revenue_rank <= 10
# MAGIC ),
# MAGIC
# MAGIC totals AS (
# MAGIC
# MAGIC     -- Calculate total 2023 revenue and combined Top 10 revenue
# MAGIC     SELECT
# MAGIC         (SELECT SUM(route_revenue) FROM route_revenue) AS total_2023_revenue,
# MAGIC         (SELECT SUM(route_revenue) FROM top_10) AS top_10_revenue
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t.revenue_rank,
# MAGIC     t.pickup_borough,
# MAGIC     t.dropoff_borough,
# MAGIC     ROUND(t.route_revenue, 2) AS route_revenue,
# MAGIC
# MAGIC     -- Share of total 2023 revenue contributed by the Top 10 pairs
# MAGIC     ROUND(
# MAGIC         100.0 * totals.top_10_revenue / totals.total_2023_revenue,
# MAGIC         2
# MAGIC     ) AS top_10_revenue_share_percent
# MAGIC
# MAGIC FROM top_10 t
# MAGIC CROSS JOIN totals
# MAGIC
# MAGIC ORDER BY t.revenue_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q4: The aim of Question 4 was to identify the highest-revenue pickup-to-drop-off borough pairs in 2023 and determine how much of the total taxi revenue was concentrated within the Top 10 routes.
# MAGIC
# MAGIC For 2023, pickup-to-drop-off borough pairs were ranked based on their total revenue (total_amount). The 10 highest-revenue borough pairs were identified, and their combined contribution to total 2023 taxi revenue was calculated.
# MAGIC
# MAGIC The Top 10 borough pairs contributed 97.03% of the total taxi revenue in 2023. The highest-revenue pair was Manhattan → Manhattan, generating approximately 670.76 million.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Business Question 5:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_trips,
# MAGIC
# MAGIC     -- Trips where the driver received a tip
# MAGIC     SUM(CASE 
# MAGIC         WHEN tip_amount > 0 THEN 1 
# MAGIC         ELSE 0 
# MAGIC     END) AS trips_with_tip,
# MAGIC
# MAGIC     -- Percentage of trips where drivers received tips
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE 
# MAGIC             WHEN tip_amount > 0 THEN 1 
# MAGIC             ELSE 0 
# MAGIC         END) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS trips_with_tip_percent
# MAGIC
# MAGIC FROM workspace.default.taxi_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q5: The aim of Question 5 was to determine how frequently drivers received tips across all trips in the final dataset.
# MAGIC
# MAGIC The query identified trips with a positive tip_amount and calculated their percentage across all trips in the final dataset.
# MAGIC
# MAGIC Drivers received tips in approximately 62.45% of all trips (616,693,994 out of 987,496,857 trips).
# MAGIC
# MAGIC Yani yüzde değişmedi (%62.45), sadece cleaning sonrası toplam ve tipped trip sayıları çok az değişti.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Business Question 6:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS trips_with_tip,
# MAGIC
# MAGIC     -- Trips where the recorded tip was at least 5 dolar
# MAGIC     SUM(CASE
# MAGIC         WHEN tip_amount >= 5 THEN 1
# MAGIC         ELSE 0
# MAGIC     END) AS trips_with_tip_at_least_5,
# MAGIC
# MAGIC     -- Percentage among trips where a tip was received
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(CASE
# MAGIC             WHEN tip_amount >= 5 THEN 1
# MAGIC             ELSE 0
# MAGIC         END) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS tip_at_least_5_percent
# MAGIC
# MAGIC FROM workspace.default.taxi_final
# MAGIC
# MAGIC WHERE tip_amount > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q6: The aim of Question 6 was to determine what proportion of tipped trips included a tip of at least $5.
# MAGIC
# MAGIC The query considered only trips with a positive tip_amount and calculated the percentage where the recorded tip was at least $5.
# MAGIC
# MAGIC Among trips where drivers received tips, approximately 13.59% had a recorded tip of at least $5 (83,812,368 out of 616,693,994 tipped trips).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Business Question 7:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH duration_bins AS (
# MAGIC     SELECT
# MAGIC         CASE
# MAGIC             WHEN duration_minutes < 5 THEN 'Under 5 mins'
# MAGIC             WHEN duration_minutes < 10 THEN '5 to <10 mins'
# MAGIC             WHEN duration_minutes < 20 THEN '10 to <20 mins'
# MAGIC             WHEN duration_minutes < 30 THEN '20 to <30 mins'
# MAGIC             WHEN duration_minutes < 60 THEN '30 to <60 mins'
# MAGIC             ELSE 'At least 60 mins'
# MAGIC         END AS duration_bin,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN duration_minutes < 5 THEN 1
# MAGIC             WHEN duration_minutes < 10 THEN 2
# MAGIC             WHEN duration_minutes < 20 THEN 3
# MAGIC             WHEN duration_minutes < 30 THEN 4
# MAGIC             WHEN duration_minutes < 60 THEN 5
# MAGIC             ELSE 6
# MAGIC         END AS bin_order,
# MAGIC
# MAGIC         speed_kmh,
# MAGIC         trip_distance,
# MAGIC         total_amount
# MAGIC
# MAGIC     FROM workspace.default.taxi_final
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     duration_bin,
# MAGIC
# MAGIC     -- Average speed in km per hour
# MAGIC     ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
# MAGIC
# MAGIC     -- Average distance per dollar
# MAGIC     ROUND(
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN total_amount > 0
# MAGIC                 THEN (trip_distance * 1.609344) / total_amount
# MAGIC             END
# MAGIC         ),
# MAGIC         2
# MAGIC     ) AS avg_distance_per_dollar
# MAGIC
# MAGIC FROM duration_bins
# MAGIC
# MAGIC GROUP BY
# MAGIC     duration_bin,
# MAGIC     bin_order
# MAGIC
# MAGIC ORDER BY bin_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q7: The aim of Question 7 was to compare trip efficiency across different trip-duration groups by analysing average speed and average distance travelled per dollar.
# MAGIC
# MAGIC Trips were classified into six duration bins, and the average speed and average distance travelled per dollar were calculated for each bin.
# MAGIC
# MAGIC The from 30 to 60 minutes bin had the highest average speed at 25.68 km/h, while trips lasting at least 60 minutes had the highest average distance per dollar at 0.62 km/$.
# MAGIC
# MAGIC Average distance per dollar generally increased with trip duration, from 0.15 km/$ for trips under 5 minutes to 0.62 km/$ for trips lasting at least 60 minutes. This indicates that longer trips generally covered more distance for each dollar paid. Average speed did not increase consistently with duration, but was generally higher for the longer-duration groups, reaching its highest level in the from 30 to 60 minute bin.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Business Question 8:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH duration_bins AS (
# MAGIC     SELECT
# MAGIC         CASE
# MAGIC             WHEN duration_minutes < 5 THEN 'Under 5 mins'
# MAGIC             WHEN duration_minutes < 10 THEN '5 to <10 mins'
# MAGIC             WHEN duration_minutes < 20 THEN '10 to <20 mins'
# MAGIC             WHEN duration_minutes < 30 THEN '20 to <30 mins'
# MAGIC             WHEN duration_minutes < 60 THEN '30 to <60 mins'
# MAGIC             ELSE 'At least 60 mins'
# MAGIC         END AS duration_bin,
# MAGIC
# MAGIC         trip_distance,
# MAGIC         total_amount
# MAGIC
# MAGIC     FROM workspace.default.taxi_final
# MAGIC ),
# MAGIC
# MAGIC bin_metrics AS (
# MAGIC     SELECT
# MAGIC         duration_bin,
# MAGIC
# MAGIC         ROUND(
# MAGIC             AVG(
# MAGIC                 CASE
# MAGIC                     WHEN total_amount > 0
# MAGIC                     THEN (trip_distance * 1.609344) / total_amount
# MAGIC                 END
# MAGIC             ),
# MAGIC             2
# MAGIC         ) AS avg_distance_per_dollar
# MAGIC
# MAGIC     FROM duration_bins
# MAGIC     GROUP BY duration_bin
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     duration_bin,
# MAGIC     avg_distance_per_dollar
# MAGIC
# MAGIC FROM bin_metrics
# MAGIC
# MAGIC ORDER BY avg_distance_per_dollar ASC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q8: The aim of Question 8 was to identify which trip-duration bin should be recommended to a taxi driver to maximise income, based on the trip efficiency results calculated in Question 7.
# MAGIC
# MAGIC Based on the results from Question 7, the “Under 5 mins” duration bin is recommended. It had the lowest average distance per dollar at 0.15 km/$, meaning that drivers travelled the least distance for each dollar earned. Therefore, among the duration bins analysed, short trips provided the highest revenue efficiency in terms of distance travelled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Business Question 9:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH duration_bins AS (
# MAGIC     SELECT
# MAGIC         CASE
# MAGIC             WHEN duration_minutes < 5 THEN 'Under 5 mins'
# MAGIC             WHEN duration_minutes < 10 THEN '5 to <10 mins'
# MAGIC             WHEN duration_minutes < 20 THEN '10 to <20 mins'
# MAGIC             WHEN duration_minutes < 30 THEN '20 to <30 mins'
# MAGIC             WHEN duration_minutes < 60 THEN '30 to <60 mins'
# MAGIC             ELSE 'At least 60 mins'
# MAGIC         END AS duration_bin,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN duration_minutes < 5 THEN 1
# MAGIC             WHEN duration_minutes < 10 THEN 2
# MAGIC             WHEN duration_minutes < 20 THEN 3
# MAGIC             WHEN duration_minutes < 30 THEN 4
# MAGIC             WHEN duration_minutes < 60 THEN 5
# MAGIC             ELSE 6
# MAGIC         END AS bin_order,
# MAGIC
# MAGIC         duration_minutes,
# MAGIC         speed_kmh,
# MAGIC         trip_distance,
# MAGIC         total_amount
# MAGIC
# MAGIC     FROM workspace.default.taxi_final
# MAGIC ),
# MAGIC
# MAGIC bin_statistics AS (
# MAGIC     SELECT
# MAGIC         duration_bin,
# MAGIC         bin_order,
# MAGIC
# MAGIC         -- Metrics from Question 7
# MAGIC         AVG(speed_kmh) AS avg_speed_kmh,
# MAGIC
# MAGIC         AVG(
# MAGIC             CASE
# MAGIC                 WHEN total_amount > 0
# MAGIC                 THEN (trip_distance * 1.609344) / total_amount
# MAGIC             END
# MAGIC         ) AS avg_distance_per_dollar,
# MAGIC
# MAGIC         -- Values needed for Question 9
# MAGIC         AVG(duration_minutes) AS avg_trip_duration_min,
# MAGIC         AVG(total_amount) AS avg_amount_per_trip
# MAGIC
# MAGIC     FROM duration_bins
# MAGIC
# MAGIC     GROUP BY
# MAGIC         duration_bin,
# MAGIC         bin_order
# MAGIC ),
# MAGIC
# MAGIC shift_estimates AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC
# MAGIC         -- (a) Trip duration plus 5 minutes unpaid time
# MAGIC         avg_trip_duration_min + 5 AS committed_time_per_trip_min,
# MAGIC
# MAGIC         -- (b) Estimated number of completed trips in 10 hours
# MAGIC         FLOOR(
# MAGIC             600 / (avg_trip_duration_min + 5)
# MAGIC         ) AS estimated_trips_10h
# MAGIC
# MAGIC     FROM bin_statistics
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     duration_bin,
# MAGIC
# MAGIC     -- Question 7 metrics
# MAGIC     ROUND(avg_speed_kmh, 2) AS avg_speed_kmh,
# MAGIC     ROUND(avg_distance_per_dollar, 2) AS avg_distance_per_dollar,
# MAGIC
# MAGIC     -- Question 9 calculations
# MAGIC     ROUND(avg_trip_duration_min, 2) AS avg_trip_duration_min,
# MAGIC     ROUND(committed_time_per_trip_min, 2) AS committed_time_per_trip_min,
# MAGIC     estimated_trips_10h,
# MAGIC     ROUND(avg_amount_per_trip, 2) AS avg_amount_per_trip,
# MAGIC
# MAGIC     -- (c) Estimated total earnings for the 10-hour shift
# MAGIC     ROUND(
# MAGIC         estimated_trips_10h * avg_amount_per_trip,
# MAGIC         2
# MAGIC     ) AS estimated_total_earnings_10h
# MAGIC
# MAGIC FROM shift_estimates
# MAGIC
# MAGIC ORDER BY bin_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Q9t:The aim of Question 9 was to identify which trip-duration bin would maximise a taxi driver’s estimated earnings during a 10-hour shift, while accounting for the 5-minute unpaid waiting and repositioning time between trips.
# MAGIC
# MAGIC The duration-bin analysis was extended by adding a 5-minute unpaid gap between trips. For each duration bin, the committed time per trip, estimated number of trips completed during a 10-hour shift, and estimated total earnings were calculated.
# MAGIC
# MAGIC The form 30 to <60 minute bin produced the highest estimated total earnings at $616.87, with approximately 13 trips completed during the 10-hour shift. Therefore, this bin is recommended for maximising estimated shift earnings.
# MAGIC
# MAGIC This differs from the Question 8 recommendation of Under 5 minutes, which was based on the lowest average distance per dollar. Although short trips were more distance-efficient, the 5-minute unpaid gap represents a larger proportion of the total committed time for shorter trips, reducing their earning efficiency over a fixed 10-hour shift. In contrast, the 30 to <60 minute bin generated a higher average amount per trip ($47.45) and also had the highest average speed (25.68 km/h), resulting in the highest estimated total shift earnings.