# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3: Machine Learning
# MAGIC
# MAGIC This section develops and evaluates machine learning models to predict the total amount of a taxi trip. The final cleaned dataset created in Part 1 is used for modelling, while the aggregated results from Part 2 Question 3 are used to construct the baseline.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import Data Set 

# COMMAND ----------

# Load the final cleaned taxi dataset
taxi_df = spark.table("workspace.default.taxi_final")

# Load the aggregated Q3 business analysis table
q3_df = spark.table("workspace.default.q3_business_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.1 Baseline Prediction
# MAGIC
# MAGIC - I used q3_df dataset for baseline model. q3_df table is created from Part 2 -Q3 business question.So, I used the answer it up to 2023 (exluding 2024) and calculate RMSE. 

# COMMAND ----------

q3_df.printSchema()

# COMMAND ----------

taxi_df.printSchema()

# COMMAND ----------

q3_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # a. Baseline Prediction

# COMMAND ----------

from pyspark.sql import functions as F

taxi_df = spark.table("workspace.default.taxi_final")
q3_df = spark.table("workspace.default.q3_business_analysis")

# COMMAND ----------

baseline_q3 = q3_df.filter(
    F.col("year_month") < "2024-01"
)

# COMMAND ----------

baseline_q3.select("year_month").agg(
    F.min("year_month").alias("min_month"),
    F.max("year_month").alias("max_month")
).show()

#excluding 2024

# COMMAND ----------

baseline_lookup = (
    baseline_q3
    .select(
        "taxi_colour",
        "pickup_borough",
        "dropoff_borough",
        "year_month",
        "weekday",
        "pickup_hour",
        F.col("avg_amount_per_trip").alias("baseline_prediction")
    )
)

# COMMAND ----------

baseline_lookup.show(10, truncate=False)

# COMMAND ----------

baseline_actuals = (
    taxi_df
    .filter(
        (F.col("pickup_datetime") >= "2014-01-01") &
        (F.col("pickup_datetime") < "2024-01-01")
    )
    .select(
        "taxi_colour",
        "pickup_borough",
        "dropoff_borough",
        F.date_format("pickup_datetime", "yyyy-MM").alias("year_month"),
        F.date_format("pickup_datetime", "EEEE").alias("weekday"),
        F.hour("pickup_datetime").alias("pickup_hour"),
        "total_amount"
    )
)

# COMMAND ----------

baseline_comparison = (
    baseline_actuals
    .join(
        baseline_lookup,
        on=[
            "taxi_colour",
            "pickup_borough",
            "dropoff_borough",
            "year_month",
            "weekday",
            "pickup_hour"
        ],
        how="left"
    )
)

# COMMAND ----------

baseline_comparison.select(
    F.sum(
        F.when(F.col("baseline_prediction").isNull(), 1).otherwise(0)
    ).alias("missing_baseline_predictions")
).show()

# COMMAND ----------

baseline_rmse = (
    baseline_comparison
    .select(
        F.sqrt(
            F.avg(
                F.pow(
                    F.col("total_amount") - F.col("baseline_prediction"),
                    2
                )
            )
        ).alias("baseline_rmse")
    )
)

baseline_rmse.show()

# COMMAND ----------

# MAGIC %md
# MAGIC - Baseline result shows that 9.7267. Then I used two different ML algorithm for prediction. And then, I compared baseline vs ml models. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Asses Models 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2.1 Select Model Features 

# COMMAND ----------

from pyspark.sql import functions as F

ml_df = (
    taxi_df
    .select(
        "total_amount",
        "taxi_colour",
        "pickup_borough",
        "dropoff_borough",
        "trip_distance",
        "duration_minutes",
        F.year("pickup_datetime").alias("year"),
        F.month("pickup_datetime").alias("month"),
        F.dayofweek("pickup_datetime").alias("weekday"),
        F.hour("pickup_datetime").alias("hour")
    )
)

# COMMAND ----------

#check to missing values

ml_df.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in ml_df.columns
]).show(truncate=False)

# COMMAND ----------

categorical_cols = [
    "taxi_colour",
    "pickup_borough",
    "dropoff_borough"
]

for c in categorical_cols:
    print(f"\nValues in {c}:")
    ml_df.groupBy(c).count().orderBy(F.desc("count")).show(50, truncate=False)

# COMMAND ----------

ml_df = ml_df.withColumn(
    "duration_minutes",
    F.col("duration_minutes").cast("double")
)

# COMMAND ----------

ml_df.printSchema()

# COMMAND ----------

final_2024_df = ml_df.filter(
    (F.col("year") == 2024) &
    (F.col("month").isin(10, 11, 12))
)

model_data_df = ml_df.filter(
    ~(
        (F.col("year") == 2024) &
        (F.col("month").isin(10, 11, 12))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2.2 Split Data Set

# COMMAND ----------

train_df, test_df = model_data_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

# COMMAND ----------

train_df.write.mode("overwrite").saveAsTable(
    "workspace.default.ml_train"
)

test_df.write.mode("overwrite").saveAsTable(
    "workspace.default.ml_test"
)

final_2024_df.write.mode("overwrite").saveAsTable(
    "workspace.default.ml_final_2024"
)

# COMMAND ----------

train_df = spark.table("workspace.default.ml_train")
test_df = spark.table("workspace.default.ml_test")
final_2024_df = spark.table("workspace.default.ml_final_2024")

# COMMAND ----------

# MAGIC %md
# MAGIC - ### For pipeline process before fit model

# COMMAND ----------

from pyspark.ml.feature import StringIndexer

taxi_indexer = StringIndexer(
    inputCol="taxi_colour",
    outputCol="taxi_colour_index",
    handleInvalid="keep"
)

pickup_indexer = StringIndexer(
    inputCol="pickup_borough",
    outputCol="pickup_borough_index",
    handleInvalid="keep"
)

dropoff_indexer = StringIndexer(
    inputCol="dropoff_borough",
    outputCol="dropoff_borough_index",
    handleInvalid="keep"
)

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder(
    inputCols=[
        "taxi_colour_index",
        "pickup_borough_index",
        "dropoff_borough_index"
    ],
    outputCols=[
        "taxi_colour_ohe",
        "pickup_borough_ohe",
        "dropoff_borough_ohe"
    ]
)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "taxi_colour_ohe",
        "pickup_borough_ohe",
        "dropoff_borough_ohe",
        "trip_distance",
        "duration_minutes",
        "year",
        "month",
        "weekday",
        "hour"
    ],
    outputCol="features"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2.3 First Model: Linear Regression

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
    featuresCol="features",
    labelCol="total_amount"
)

# COMMAND ----------

from pyspark.ml import Pipeline

lr_pipeline = Pipeline(
    stages=[
        taxi_indexer,
        pickup_indexer,
        dropoff_indexer,
        encoder,
        assembler,
        lr
    ]
)

# COMMAND ----------

import time

start_time = time.time()

lr_model = lr_pipeline.fit(train_df)

lr_training_time = time.time() - start_time

print(f"LR training time: {lr_training_time:.2f} seconds")

# COMMAND ----------

lr_predictions = lr_model.transform(test_df)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

lr_evaluator = RegressionEvaluator(
    labelCol="total_amount",
    predictionCol="prediction",
    metricName="rmse"
)

lr_rmse = lr_evaluator.evaluate(lr_predictions)

print(f"Linear Regression Test RMSE: {lr_rmse:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2.3.1 Hyperparameter for Tuning

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr_tuned = LinearRegression(
    featuresCol="features",
    labelCol="total_amount",
    regParam=0.1,
    elasticNetParam=0.0
)

# COMMAND ----------

lr_tuned_pipeline = Pipeline(
    stages=[
        taxi_indexer,
        pickup_indexer,
        dropoff_indexer,
        encoder,
        assembler,
        lr_tuned
    ]
)

# COMMAND ----------

lr_tuned_model = lr_tuned_pipeline.fit(train_df)

# COMMAND ----------

lr_tuned_predictions = lr_tuned_model.transform(test_df)

lr_tuned_rmse = lr_evaluator.evaluate(lr_tuned_predictions)

print(f"Tuned Linear Regression Test RMSE: {lr_tuned_rmse:.3f}")

# COMMAND ----------

print(f"Default LR RMSE: {lr_rmse:.10f}")
print(f"Tuned LR RMSE:   {lr_tuned_rmse:.10f}")
print(f"Difference:      {lr_tuned_rmse - lr_rmse:.10f}")

# COMMAND ----------

# MAGIC %md
# MAGIC #Check to small dataset for hyperparameter tuning:
# MAGIC To reduce computational cost, hyperparameter testing was performed on a small subset of the training data rather than repeatedly fitting multiple models on the full dataset. Several combinations of `regParam` and `elasticNetParam` were evaluated on a smaller training subset to reduce computational cost. The default Linear Regression configuration (`regParam = 0.0`, `elasticNetParam = 0.0`) achieved the lowest validation RMSE. Increasing regularisation did not improve model performance, with RMSE increasing slightly as stronger regularisation was applied. Therefore, the default Linear Regression configuration was retained for the full dataset.

# COMMAND ----------

# Small subset for hyperparameter tuning
tuning_df = train_df.sample(
    withReplacement=False,
    fraction=0.00001,
    seed=42
)

# COMMAND ----------

tuning_train_df, tuning_valid_df = tuning_df.randomSplit(
    [0.8, 0.2],
    seed=42
)

# COMMAND ----------

param_combinations = [
    (0.0, 0.0),
    (0.01, 0.0),
    (0.1, 0.0),
    (0.01, 0.5),
    (0.1, 0.5),
    (0.01, 1.0),
    (0.1, 1.0)
]

# COMMAND ----------

results = []

for reg_param, elastic_net_param in param_combinations:

    lr_temp = LinearRegression(
        featuresCol="features",
        labelCol="total_amount",
        regParam=reg_param,
        elasticNetParam=elastic_net_param
    )

    temp_pipeline = Pipeline(
        stages=[
            taxi_indexer,
            pickup_indexer,
            dropoff_indexer,
            encoder,
            assembler,
            lr_temp
        ]
    )

    # Fit on tuning training subset
    temp_model = temp_pipeline.fit(tuning_train_df)

    # Evaluate on tuning validation subset
    temp_predictions = temp_model.transform(tuning_valid_df)

    temp_rmse = lr_evaluator.evaluate(temp_predictions)

    results.append(
        (reg_param, elastic_net_param, temp_rmse)
    )

# COMMAND ----------

results_df = spark.createDataFrame(
    results,
    ["regParam", "elasticNetParam", "RMSE"]
)

display(
    results_df.orderBy("RMSE")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # - Tried to full data set :

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
import time

# Same hyperparameter combinations tested on the small tuning subset
param_combinations = [
    (0.0, 0.0),
    (0.01, 0.0),
    (0.1, 0.0),
    (0.01, 0.5),
    (0.1, 0.5),
    (0.01, 1.0),
    (0.1, 1.0)
]

full_lr_results = []

for reg_param, elastic_net_param in param_combinations:

    print(
        f"Training: regParam={reg_param}, "
        f"elasticNetParam={elastic_net_param}"
    )

    lr_temp = LinearRegression(
        featuresCol="features",
        labelCol="total_amount",
        regParam=reg_param,
        elasticNetParam=elastic_net_param
    )

    temp_pipeline = Pipeline(
        stages=[
            taxi_indexer,
            pickup_indexer,
            dropoff_indexer,
            encoder,
            assembler,
            lr_temp
        ]
    )

    start_time = time.time()

    temp_model = temp_pipeline.fit(train_df)

    training_time = time.time() - start_time

    temp_predictions = temp_model.transform(test_df)

    temp_rmse = lr_evaluator.evaluate(temp_predictions)

    full_lr_results.append(
        (
            float(reg_param),
            float(elastic_net_param),
            float(temp_rmse),
            float(training_time)
        )
    )

    print(
        f"RMSE: {temp_rmse:.4f} | "
        f"Training time: {training_time / 60:.2f} minutes"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC - Linear Regression hyperparameter tuning focused on regParam and elasticNetParam. The results showed that regularisation did not improve predictive performance, with the default configuration (regParam=0.0, elasticNetParam=0.0) achieving the lowest or comparable RMSE of approximately 7.351. Therefore, the default configuration was retained for the final Linear Regression model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2.4 Second Model: Random Forest 

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="total_amount",
    seed=42
)

# COMMAND ----------

rf_pipeline = Pipeline(
    stages=[
        taxi_indexer,
        pickup_indexer,
        dropoff_indexer,
        encoder,
        assembler,
        rf
    ]
)

# COMMAND ----------

import time

start_time = time.time()

rf_model = rf_pipeline.fit(train_df)

rf_training_time = time.time() - start_time

print(f"RF training time: {rf_training_time:.2f} seconds")

# COMMAND ----------

rf_predictions = rf_model.transform(test_df)

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

rf_evaluator = RegressionEvaluator(
    labelCol="total_amount",
    predictionCol="prediction",
    metricName="rmse"
)

rf_rmse = rf_evaluator.evaluate(rf_predictions)

print(f"Random Forest Test RMSE: {rf_rmse:.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Best Model Selection 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Linear Regression was selected as the final model. According to ;
# MAGIC - •	Predictive Performance: Linear Regression achieved a lower test RMSE of 7.351, compared with 7.422 for Random Forest. Although the difference was relatively small, Random Forest did not improve predictive accuracy.
# MAGIC - •	Computational Cost: Linear Regression required approximately 13.3 minutes (798.82 seconds) to train, whereas Random Forest required approximately 73.2 minutes (4,392.55 seconds). It is also less computationally complex and easier to interpret than a Random Forest consisting of multiple decision trees. Since Random Forest required considerably more processing time without improving predictive accuracy, Linear Regression provided the stronger balance for this large-scale application.
# MAGIC - •	Model Complexity: Linear Regression has a simpler model structure, whereas Random Forest combines predictions from multiple decision trees. For a large-scale dataset, the lower complexity of Linear Regression also reduces computational requirements. 
# MAGIC - •	Explainability: Linear Regression provides a more transparent model structure, making the relationship between the input features and the predicted total_amount easier to interpret. Random Forest is comparatively more difficult to interpret because predictions are generated from an ensemble of decision trees.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 The best model prediction : For October, November and December 2024 trips dataset

# COMMAND ----------

# Predict total_amount for October, November and December 2024
# We created final_2024_df from taxi_final main dataset. 
final_2024_predictions = lr_model.transform(final_2024_df)

# Calculate RMSE
final_2024_rmse = lr_evaluator.evaluate(final_2024_predictions)

print(f"Linear Regression RMSE on Oct-Dec 2024: {final_2024_rmse:.3f}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# In Part 3(a), the baseline RMSE was calculated using data up to 2023, excluding 2024.
# To fairly compare the baseline with the final Linear Regression predictions,
# the baseline approach is now applied to the same Oct-Dec 2024 holdout period.

# Create a historical baseline using data up to 2023.
# The same calendar months across different years are combined.

historical_baseline = (
    q3_df
    .filter(F.col("year_month") < "2024-01")
    .withColumn(
        "month",
        F.month(
            F.to_date(
                F.concat(F.col("year_month"), F.lit("-01"))
            )
        )
    )
    .groupBy(
        "taxi_colour",
        "pickup_borough",
        "dropoff_borough",
        "month",
        "weekday",
        "pickup_hour"
    )
    .agg(
        (
            F.sum(F.col("avg_amount_per_trip") * F.col("total_trips"))
            / F.sum("total_trips")
        ).alias("baseline_prediction")
    )
)

# COMMAND ----------

# Prepare the actual Oct-Dec 2024 trips using the same grouping format as Part 2 Q3.

baseline_2024_actuals = (
    taxi_df
    .filter(
        (F.col("pickup_datetime") >= "2024-10-01") &
        (F.col("pickup_datetime") < "2025-01-01")
    )
    .select(
        "taxi_colour",
        "pickup_borough",
        "dropoff_borough",
        F.month("pickup_datetime").alias("month"),
        F.date_format("pickup_datetime", "EEEE").alias("weekday"),
        F.hour("pickup_datetime").alias("pickup_hour"),
        "total_amount"
    )
)

# COMMAND ----------

# Join the 2024 trips with the historical baseline predictions.

baseline_2024_comparison = (
    baseline_2024_actuals
    .join(
        historical_baseline,
        on=[
            "taxi_colour",
            "pickup_borough",
            "dropoff_borough",
            "month",
            "weekday",
            "pickup_hour"
        ],
        how="left"
    )
)

# COMMAND ----------

# Calculate the baseline RMSE for Oct-Dec 2024
# Only trips with an available historical baseline prediction are evaluated.

baseline_2024_rmse = (
    baseline_2024_comparison
    .filter(F.col("baseline_prediction").isNotNull())
    .select(
        F.sqrt(
            F.avg(
                F.pow(
                    F.col("total_amount") - F.col("baseline_prediction"),
                    2
                )
            )
        ).alias("baseline_2024_rmse")
    )
)

baseline_2024_rmse.show()