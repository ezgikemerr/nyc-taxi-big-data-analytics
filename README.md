# NYC Taxi Trips | Big Data Engineering & Machine Learning

**Databricks · Apache Spark · Spark SQL · PySpark MLlib · Data Cleaning · Business Analytics**

## Project Overview

This project builds an end-to-end big data analytics workflow using New York City Taxi and Limousine Commission (TLC) **Yellow and Green Taxi** trip records from **2014–2024**. Using Databricks and Apache Spark, the project explores and cleans large-scale trip data, integrates taxi-zone reference information, answers operational business questions, and evaluates machine learning models for predicting trip payment amounts.

## Dependencies

- Python 3.x
- Apache Spark
- PySpark
- PySpark MLlib
- Spark SQL
- Databricks (Free Edition recommended)

The project was developed using Databricks, Apache Spark, Spark SQL, and PySpark MLlib.

For local environments, install the Python dependencies using:

```bash
pip install -r requirements.txt
```

**Note:** PySpark and Spark MLlib are already available in Databricks. Additional installation is not required when running the notebooks in Databricks.

## Usage

1. Clone the repository:

```bash
git clone https://github.com/ezgikemerr/nyc-taxi-big-data-analytics.git
cd nyc-taxi-big-data-analytics
```
2. Download the NYC Yellow Taxi, Green Taxi, and Taxi Zone Lookup datasets from the official NYC TLC website.

3. Upload the datasets to Databricks Volumes and update the file paths in the notebooks.

4. Import the notebooks into Databricks.

5. Run the notebooks in the following order:

   - Part 1: Data Preparation
   - Part 2: Business Analytics
   - Part 3: Machine Learning

6. Review the generated tables, business analysis results, and machine learning model evaluations.

**Note:** The project processes approximately 991 million raw taxi trip records. Running the complete pipeline requires substantial storage and computational resources. A smaller dataset is recommended for initial testing.

## Dataset

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Trip records:** Yellow and Green Taxi Parquet datasets
- **Reference data:** TLC Taxi Zone Lookup (pickup and drop-off locations)
- **Period:** 2014–2024
- **Raw records:** 991,467,464
- **Cleaned records:** 987,496,857 (approximately **99.6% retained**)

The trip records contain pickup/drop-off timestamps, distance, passenger counts, location IDs, payment details, and other trip attributes. The location lookup adds borough and zone information.


## Technology Stack

| Area | Tools |
| --- | --- |
| Platform | Databricks |
| Distributed processing | Apache Spark, PySpark |
| Data preparation and analytics | Spark SQL |
| Machine learning | PySpark MLlib |
| Data formats | Parquet, CSV |
| Evaluation | Root Mean Squared Error (RMSE) |

## Workflow

```text
Yellow Taxi Parquet ─┐
                     ├─► Explore & clean ─► Combine trips ─► Join TLC taxi zones
Green Taxi Parquet ──┘                                        │
Taxi Zone Lookup ──────────────────────────────────────────────┘
                                                               │
                                                       Clean analytical table
                                                               │
                                              ┌────────────────┴───────────────┐
                                              ▼                                ▼
                                         Business analysis              ML regression
                                                                           │
                                                              Model evaluation &
                                                              2024 holdout prediction
```

### 1. Data Exploration and Cleaning

Spark SQL was used to investigate and address inconsistent timestamps, negative or extreme trip speeds, implausible durations and distances, and extreme payment values. A **120 km/h upper threshold** was selected for calculated *average trip speed* as a conservative data-quality rule—not as a legal NYC speed limit. Missing values in nonessential fields and zero-distance trips were not automatically removed. Yellow and Green Taxi data were standardised, combined, and enriched with pickup/drop-off borough information.

### 2. Business Analytics

The analysis explored:

- Monthly trip volumes, busiest weekdays/hours, passenger counts, and average payments.
- Trip duration, distance, and speed differences between Yellow and Green Taxis.
- Revenue patterns across borough pairs, months, weekdays, and pickup hours.
- Top revenue-generating routes and driver tipping behaviour.
- Trip-duration efficiency and estimated earnings for a **10-hour shift**, allowing **5 unpaid minutes between trips**.

**Selected findings:**

- **Manhattan → Manhattan** generated approximately **$670.8 million** in recorded total payments in 2023, ranking first among borough pairs.
- Approximately **62.45%** of trips recorded a positive tip; **13.59%** of tipped trips had tips of at least $5.
- Although trips under five minutes had the lowest kilometres-per-dollar metric, the **30–<60 minute** duration group produced the highest estimated shift earnings (**$616.87**) under the stated assumptions.

> **Location note:** The TLC Taxi Zone Lookup explicitly includes locations classified as `Unknown`. These categories are not, by themselves, evidence of invalid trip records.

### 3. Machine Learning

Two regression algorithms were trained and compared to predict **`total_amount`**:

- **Linear Regression**
- **Random Forest Regression**

Features included taxi colour, pickup/drop-off borough, trip distance, duration, and calendar/time attributes. Components that directly make up the target amount (such as fare and tip amounts) were excluded to reduce target leakage. Training and evaluation used historical data, with **October–December 2024** reserved as a final holdout period.

| Evaluation | RMSE |
| --- | ---: |
| Historical grouped-average baseline | 9.727 |
| Linear Regression — test set | **7.351** |
| Random Forest — test set | 7.422 |
| Linear Regression — Oct–Dec 2024 holdout | **14.873** |
| Historical baseline — Oct–Dec 2024 holdout | 19.700 |

**Linear Regression was selected** because it achieved slightly lower test RMSE while requiring substantially less training time (approximately **13.3 minutes** versus **73.2 minutes** for Random Forest). Regularisation settings were also explored for Linear Regression; they did not yield a meaningful improvement over the default configuration.

The final holdout results show that the selected model outperformed the historical baseline, although prediction errors were higher on the later 2024 period than on the test set.

## Project Structure

The project is organised around three stages:

## Project at a Glance

| Stage | Work completed | Key outcome |
| --- | --- | --- |
| **Part 1 — Data Preparation** | Explore Parquet files, apply justified cleaning rules, align Yellow/Green schemas, union records and enrich with taxi-zone lookup | **987,496,857** final trips (approximately **99.60%** retained) |
| **Part 2 — Business Analysis** | Nine Spark SQL business questions on monthly activity, taxi characteristics, geographic revenue, tips, efficiency and shift earnings | Highest estimated 10-hour shift earnings: **$616.87** for 30–<60-minute trips |
| **Part 3 — Machine Learning** | Historical baseline, leakage-aware features, Linear Regression and Random Forest, hyperparameter experiments and final 2024 holdout | **Linear Regression** test RMSE **7.351**; final holdout RMSE **14.873** versus baseline **19.700** |

| File | Purpose |
| --- | --- |
| `Part1_Data_Preparation_notebook1.py` | Exploration, cleaning, schema alignment and location enrichment |
| `Part2_BusinessQuestions_notebook2.py` | Spark SQL business questions and results |
| `Part 3_MachineLearning_notebook3.py` | Baseline, ML preprocessing, model training and evaluation |
| `ANALYSIS.md` | Detailed results, interpretations and suggested visualisations |

The notebook exports may also be provided in `.html` format for convenient browser viewing. Files are listed using their original export names; adjust paths if you organise them into a `notebooks/` folder.

The original NYC TLC datasets are **not included** in this repository due to their size. Download the source data from TLC and configure the appropriate Databricks storage paths before running notebooks in order: **download → Part 1 → Part 2 → Part 3**. Processing nearly one billion records requires distributed compute; execution times will depend on the available cluster and configuration.

## Results and Limitations

The model's **test RMSE of 7.351** increased to **14.873** on the unseen October–December 2024 period, although it still outperformed the historical baseline (**19.700**) on that same period. This highlights the importance of evaluating models on later data and monitoring changes in trip and fare patterns.

For the complete analysis, see **[ANALYSIS.md](ANALYSIS.md)**.