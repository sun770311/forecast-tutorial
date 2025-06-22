import pandas as pd
import numpy as np
from google.cloud import bigquery
from setup import arima_dataset_path, PROJECT_ID

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Load predictions
query = f"SELECT * FROM `{arima_dataset_path}.evaluated_examples`"
df = client.query(query).to_dataframe()

# Extract numeric predicted value
df["predicted_sales"] = df["predicted_sales"].apply(lambda x: x["value"] if isinstance(x, dict) else None)

# Drop rows with missing values
df = df.dropna(subset=["sales", "predicted_sales"])

# Compute metrics
y_true = df["sales"]
y_pred = df["predicted_sales"]

mae = np.mean(np.abs(y_true - y_pred))
rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100

# Output
print("Custom Evaluation Metrics:")
print(f"MAE  = {mae:.2f}")
print(f"RMSE = {rmse:.2f}")
print(f"MAPE = {mape:.2f}%")

"""
Custom Evaluation Metrics (can also view in Vertex AI dashboard):
MAE  = 35.89
RMSE = 71.49
MAPE = 4.58%
"""