# NYC Taxi Data — Detailed Analysis

**Project:** Big Data Engineering · NYC Yellow and Green Taxi Trips · 2014–2024  
**Tools:** Databricks, Apache Spark, Spark SQL, PySpark MLlib

[← Project overview (README)](README.md)


## 1. Data Preparation and Quality

| Dataset | Raw records | Records after cleaning |
| --- | ---: | ---: |
| Yellow Taxi | 907,982,776 | 904,655,251 |
| Green Taxi | 83,484,688 | 82,841,606 |
| **Total** | **991,467,464** | **987,496,857** |

Approximately **0.40%** of records were removed; approximately **99.60%** were retained.

### Cleaning decisions

| Issue | Decision | Rationale |
| --- | --- | --- |
| Drop-off not after pickup | Remove | Invalid trip chronology |
| Dates outside 2014–2024 | Remove | Outside study period |
| Negative trip distance / speed | Remove | Inconsistent with physical trip measurements |
| Calculated average speed above 120 km/h | Remove | Conservative upper threshold above typical posted limits; legal speeding alone does not prove a trip invalid |
| Duration above 360 minutes | Remove | Excludes extreme durations and possible timestamp anomalies |
| Distance above 100 miles | Remove | Excludes extreme distance anomalies |
| `total_amount` above $10,000 | Remove | Excludes 103 extreme positive monetary observations after other checks |
| Zero-distance trips | Retain | Not sufficient evidence of invalidity |
| Negative `total_amount` | Retain | Could reflect refunds, reversals or corrections |
| Missing non-essential fields | Retain | Avoids unnecessary loss of otherwise usable records |

The cleaned taxi schemas were aligned and combined using `UNION ALL`. Pickup and drop-off location IDs were each joined to the TLC Taxi Zone Lookup. **`Unknown`** boroughs in the revenue results reflect classifications present in the original TLC lookup, rather than automatically indicating invalid trips or failed joins.

**Suggested figure:** `images/data_pipeline.png` — ingestion → cleaning → union → location enrichment → analytics → ML.

## 2. Business Analysis

### 2.1 Monthly taxi activity (Question 1)

Monthly recorded trip counts were frequently above **14 million** in 2014–2015, with a pronounced decline in 2020: **6,673,908** trips in February 2020, **3,219,013** in March, and **272,550** in April. Busiest pickup hours were commonly **18:00–19:00**, with shifts towards **14:00–15:00** during some low-activity pandemic months. Average passengers per trip decreased from around **1.7** in 2014 to **1.3–1.4** in 2024, while average payment per trip increased from approximately **$14–$16** to **$27–$29**.

**Data caveat:** March 2017 shows **1,148,155** trips; source-data investigation found no Yellow Taxi records for that month, so the apparent drop should not be interpreted as a real demand collapse.

**Suggested figure:** `images/monthly_trips.png` — monthly recorded trips, 2014–2024, with a 2020 annotation.

### 2.2 Yellow vs Green taxi characteristics (Question 2)

| Measure | Yellow Taxi | Green Taxi |
| --- | ---: | ---: |
| Average duration (minutes) | 14.35 | 13.73 |
| Median duration (minutes) | 11.23 | 10.52 |
| Average distance (km) | 4.87 | 4.81 |
| Median distance (km) | 2.74 | 3.07 |
| Average calculated speed (km/h) | 18.77 | 20.19 |
| Median calculated speed (km/h) | 16.52 | 18.45 |

Yellow trips were slightly longer on average; Green trips had higher typical calculated speeds.

**Suggested figure:** `images/taxi_type_comparison.png` — paired bars for duration, distance and speed (separate scales/panels if charting).

### 2.3 Revenue by location and time (Questions 3–4)

Question 3 grouped trips by **taxi colour, pickup borough, drop-off borough, year-month, weekday and pickup hour**, calculating trip count, average distance, average payment and total payment. The resulting aggregated table also supported the historical baseline in Part 3.

**Top ten pickup → drop-off borough pairs by 2023 revenue:**

| Rank | Route | Revenue ($) |
| ---: | --- | ---: |
| 1 | Manhattan → Manhattan | 670,762,273.99 |
| 2 | Queens → Manhattan | 171,108,845.85 |
| 3 | Manhattan → Queens | 78,598,926.91 |
| 4 | Queens → Brooklyn | 38,046,494.80 |
| 5 | Queens → Queens | 36,448,060.83 |
| 6 | Manhattan → Brooklyn | 34,592,392.04 |
| 7 | Queens → Unknown | 13,625,560.12 |
| 8 | Manhattan → EWR | 13,012,946.26 |
| 9 | Unknown → Unknown | 10,000,990.39 |
| 10 | Manhattan → Unknown | 6,584,114.66 |

Together these ten pairs accounted for **97.03%** of 2023 recorded taxi revenue. `Unknown` is a TLC lookup classification; it does not itself prove a record is invalid.

**Suggested figure:** `images/top10_revenue.png` — horizontal bar chart ranked by revenue.

### 2.4 Tipping behaviour (Questions 5–6)

| Measure | Trips | Share |
| --- | ---: | ---: |
| All cleaned trips | 987,496,857 | 100% |
| Trips with a positive recorded tip | 616,693,994 | **62.45% of all trips** |
| Tipped trips with a tip ≥ $5 | 83,812,368 | **13.59% of tipped trips** |

These are shares of **recorded** tips in the dataset; they should not be read as a complete measure of cash tipping behaviour.

**Suggested figure:** `images/tipping.png`, `images/tipping2.png` — two clearly labelled percentage bars using their different denominators.

### 2.5 Trip duration and efficiency (Questions 7–8)

**Suggested figure:** `images/tripdurationandefficiency.png`.

**Question 8 recommendation:** Under-5-minute trips had the lowest distance per dollar, making them attractive **on that efficiency measure alone**. This does **not** establish that they generate the highest earnings over a full shift.

### 2.6 Estimated 10-hour shift earnings (Question 9)

The shift simulation assumed **600 working minutes** and **five minutes of unpaid waiting/repositioning time per trip**.

| Duration bin | Avg trip duration (min) | Estimated trips | Avg amount per trip ($) | Estimated shift earnings ($) |
| --- | ---: | ---: | ---: | ---: |
| Under 5 min | 3.35 | 71 | 8.18 | 581.07 |
| 5–<10 min | 7.42 | 48 | 10.23 | 491.20 |
| 10–<20 min | 14.18 | 31 | 16.24 | 503.47 |
| 20–<30 min | 24.11 | 20 | 27.56 | 551.16 |
| **30–<60 min** | **39.33** | **13** | **47.45** | **616.87** |
| At least 60 min | 75.07 | 7 | 70.39 | 492.76 |

**Final recommendation:** **30–<60-minute** trips generated the highest estimated shift earnings (**$616.87**). The fixed five-minute gap consumes a larger share of working time for very short trips, changing the recommendation from Question 8.

**Suggested figure:** `images/driver_earnings.png` — estimated earnings by duration bin, highlighting 30–<60 minutes.

## 3. Machine Learning

### 3.1 Objective and features

The target was the continuous trip payment field **`total_amount`**. Features included `taxi_colour`, `pickup_borough`, `dropoff_borough`, `trip_distance`, `duration_minutes`, `year`, `month`, `weekday` and `hour`.

Target-derived monetary components such as fares, tips, tolls, taxes and surcharges were excluded to prevent leakage. Spark ML pipelines applied categorical indexing, one-hot encoding and vector assembly.

### 3.2 Data splitting and baseline

Trips from **October–December 2024** were held out for final evaluation. Other eligible trips were split **80% training / 20% testing** with random seed 42. The historical baseline used average trip payments from pre-2024 data, grouped by geographic and temporal characteristics. Its historical benchmark RMSE was approximately **9.727**.

### 3.3 Model comparison

| Model | Internal test RMSE ↓ | Training time |
| --- | ---: | ---: |
| Historical grouped-average baseline | 9.727 | Not applicable |
| **Linear Regression** | **7.351** | **13.3 min** |
| Random Forest Regression | 7.422 | 73.2 min |

Linear Regression achieved a slightly lower test RMSE and trained substantially faster than Random Forest. It was selected as the best balance of predictive performance, computational cost, complexity and explainability.


### 3.4 Hyperparameter experiments

Linear Regression experiments tested combinations of **`regParam`** (regularisation strength) and **`elasticNetParam`** (L1/L2 mixture). Experiments began on a small training sample to reduce computational cost, with further full-data checks. Regularisation did not meaningfully improve the default Linear Regression configuration (`regParam=0`, `elasticNetParam=0`). Random Forest was also considered with alternative tree-count and depth settings; the final reported model comparison uses the recorded main-model results above.

### 3.5 Final October–December 2024 holdout

| Method | Final holdout RMSE ↓ |
| --- | ---: |
| Historical grouped-average baseline | 19.700 |
| **Selected Linear Regression** | **14.873** |

Linear Regression outperformed the historical baseline on the **same unseen three-month period**. However, its error increased compared with the internal test RMSE (**7.351 → 14.873**), suggesting that historical relationships may not transfer unchanged to later months. Predictions should therefore be treated as estimates, with periodic monitoring and retraining recommended.


## 4. Key Takeaways and Limitations

1. **Scale:** Spark enabled data preparation and analysis on approximately one billion raw trip records.
2. **Data quality:** Conservative, justified thresholds removed a small share of records while retaining unusual but potentially valid trips.
3. **Operations:** Geographic revenue is concentrated, and shift earnings depend on unpaid time—not just distance efficiency.
4. **Modelling:** The simpler Linear Regression model performed slightly better than Random Forest at much lower training cost.
5. **Generalisation:** The final 2024 holdout showed higher prediction error than the internal test set, reinforcing the need for monitoring over time.

**Interpretation note:** The 10-hour shift results are scenario-based estimates, not guaranteed take-home pay. Revenue figures are based on recorded `total_amount` and should not be interpreted as driver net profit.

## 5. Sources

- [NYC Taxi and Limousine Commission — TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [NYC311 — Speed Limits](https://portal.311.nyc.gov/)

