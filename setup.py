# Create BigQuery datasets for the ARIMA model

PROJECT_ID = ""
LOCATION = ""
DATA_LOCATION = "US"
BUCKET_NAME = ""
BUCKET_URI = f"gs://{BUCKET_NAME}"
SERVICE_ACCOUNT = ""

import json
import os
import urllib
import uuid

from google.cloud import aiplatform, bigquery
from google_cloud_pipeline_components.v1.automl.forecasting import utils 

aiplatform.init(project=PROJECT_ID, location=LOCATION, staging_bucket=BUCKET_URI)

# Create a BigQuery dataset for the ARIMA model

arima_dataset_name = "forecasting_demo_arima"
arima_dataset_path = ".".join([PROJECT_ID, arima_dataset_name])

# Must be same location as TRAINING_DATASET_BQ_PATH.
client = bigquery.Client(project=PROJECT_ID)
bq_dataset_pre = bigquery.Dataset(arima_dataset_path)
bq_dataset_pre.location = DATA_LOCATION
try:
    bq_dataset = client.create_dataset(bq_dataset_pre)
except:
    bq_dataset = client.get_dataset(bq_dataset_pre)
print(f"Created bigquery dataset {arima_dataset_path} in {DATA_LOCATION}")

# Define subqueries that creates the base sales data
base_data_query = """
  WITH 

    -- Create time series for each product + store with some covariates.
    time_series AS (
      SELECT
        CONCAT("id_", store_id, "_", product_id) AS id,
        CONCAT('store_', store_id) AS store,
        CONCAT('product_', product_id) AS product,
        date,
        -- Advertise 1/100 products.
        IF(
          ABS(MOD(FARM_FINGERPRINT(CONCAT(product_id, date)), 100)) = 0,
          1,
          0
        ) AS advertisement,
        -- Mark Thanksgiving sales as holiday sales.
        IF(
          EXTRACT(DAYOFWEEK FROM date) = 6
            AND EXTRACT(MONTH FROM date) = 11
            AND EXTRACT(DAY FROM date) BETWEEN 23 AND 29,
          1,
          0
        ) AS holiday,
        -- Set when each data split ends.
        CASE
          WHEN date < '2019-09-01' THEN 'TRAIN'
          WHEN date < '2019-10-01' THEN 'VALIDATE'
          WHEN date < '2019-11-01' THEN 'TEST'
          ELSE 'PREDICT'
        END AS split,
      -- Generate the sales with one SKU per date.
      FROM
        UNNEST(GENERATE_DATE_ARRAY('2017-01-01', '2019-12-01')) AS date
      CROSS JOIN
        UNNEST(GENERATE_ARRAY(0, 10)) AS product_id
      CROSS JOIN
        UNNEST(GENERATE_ARRAY(0, 3)) AS store_id  
    ),
    
    -- Randomly determine factors that contribute to how syntheic sales are calculated. 
    time_series_sales_factors AS (
      SELECT
        *,
        ABS(MOD(FARM_FINGERPRINT(product), 10)) AS product_factor,
        ABS(MOD(FARM_FINGERPRINT(store), 10)) AS store_factor,
        [1.6, 0.6, 0.8, 1.0, 1.2, 1.8, 2.0][
          ORDINAL(EXTRACT(DAYOFWEEK FROM date))] AS day_of_week_factor,
        1 +  SIN(EXTRACT(MONTH FROM date) * 2.0 * 3.14 / 24.0) AS month_factor,    
        -- Advertised products have increased sales factors for 5 days.
        CASE
          WHEN LAG(advertisement, 0) OVER w = 1.0 THEN 1.2
          WHEN LAG(advertisement, 1) OVER w = 1.0 THEN 1.8
          WHEN LAG(advertisement, 2) OVER w = 1.0 THEN 2.4
          WHEN LAG(advertisement, 3) OVER w = 1.0 THEN 3.0
          WHEN LAG(advertisement, 4) OVER w = 1.0 THEN 1.4
          ELSE 1.0
        END AS advertisement_factor,
        IF(holiday = 1.0, 2.0, 1.0) AS holiday_factor,
        0.001 * ABS(MOD(FARM_FINGERPRINT(CONCAT(product, store, date)), 100)) AS noise_factor
      FROM
        time_series
      WINDOW w AS (PARTITION BY id ORDER BY date)
    ),
  
    -- Use factors to calculate synthetic sales for each time series. 
    base_data AS (
      SELECT
        id,
        store,
        product,
        date,
        split,
        advertisement,
        holiday,
        (
          (1 + store_factor) 
          * (1 + product_factor) 
          * (1 + month_factor + day_of_week_factor) 
          * (
            1.0 
            + 2.0 * advertisement_factor 
            + 3.0 * holiday_factor 
            + 5.0 * noise_factor
          )
        ) AS sales
      FROM
        time_series_sales_factors
      )
"""

TRAINING_DATASET_BQ_PATH = f"{arima_dataset_path}.train"
PREDICTION_DATASET_BQ_PATH = f"{arima_dataset_path}.pred"

train_query = f"""
    CREATE OR REPLACE TABLE `{arima_dataset_path}.train` AS
    {base_data_query}
    SELECT *
    FROM base_data
    WHERE split != 'PREDICT'
"""
client.query(train_query).result()
print(f"Created {TRAINING_DATASET_BQ_PATH}.")

pred_query = f"""
    CREATE OR REPLACE TABLE `{arima_dataset_path}.pred` AS
    {base_data_query}
    SELECT *
    FROM base_data
    WHERE split = 'TEST'

    UNION ALL

    SELECT * EXCEPT (sales), NULL AS sales
    FROM base_data
    WHERE split = 'PREDICT'
"""
client.query(pred_query).result()
print(f"Created {PREDICTION_DATASET_BQ_PATH}.")

# Look at the training data
query = f"SELECT * FROM `{arima_dataset_path}.train` LIMIT 10"
client.query(query).to_dataframe().head()

# Look at the prediction data
query = f"SELECT * FROM `{arima_dataset_path}.pred` LIMIT 10"
client.query(query).to_dataframe().head()