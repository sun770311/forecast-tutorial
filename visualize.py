import json
import urllib.parse
from setup import PROJECT_ID  # Ensure setup.py defines PROJECT_ID


def _sanitize_bq_uri(bq_uri: str):
    if bq_uri.startswith("bq://"):
        bq_uri = bq_uri[5:]
    return bq_uri.replace(":", ".")


def get_data_studio_link(
    batch_prediction_bq_input_uri: str,
    batch_prediction_bq_output_uri: str,
    time_column: str,
    time_series_identifier_column: str,
    target_column: str,
):
    """Creates a Looker Studio link to visualize forecast vs historical."""
    batch_prediction_bq_input_uri = _sanitize_bq_uri(batch_prediction_bq_input_uri)
    batch_prediction_bq_output_uri = _sanitize_bq_uri(batch_prediction_bq_output_uri)

    query = f"""
        SELECT
        CAST({time_column} AS DATETIME) AS timestamp_col,
        CAST({time_series_identifier_column} AS STRING) AS time_series_identifier_col,
        CAST({target_column} AS NUMERIC) AS historical_values,
        NULL AS predicted_values
        FROM `{batch_prediction_bq_input_uri}`

        UNION ALL

        SELECT
        CAST(date AS DATETIME) AS timestamp_col,
        CAST(id AS STRING) AS time_series_identifier_col,
        NULL AS historical_values,
        CAST(predicted_sales.value AS NUMERIC) AS predicted_values
        FROM `{batch_prediction_bq_output_uri}`
    """

    params = {
        "templateId": "067f70d2-8cd6-4a4c-a099-292acd1053e8",  # Google’s template
        "ds0.connector": "BIG_QUERY",
        "ds0.projectId": PROJECT_ID,
        "ds0.billingProjectId": PROJECT_ID,
        "ds0.type": "CUSTOM_QUERY",
        "ds0.sql": query,
    }

    base_url = "https://datastudio.google.com/c/u/0/reporting"
    url_params = urllib.parse.urlencode({"params": json.dumps(params)})
    return f"{base_url}?{url_params}"


# ======== Your actual table and prediction table names ========
actuals_table = ""
predictions_table = ""

# ======== Time series columns ========
time_column = "date"
time_series_identifier_column = "id"
target_column = "sales"

# ======== Generate and print the visualization link ========
link = get_data_studio_link(
    batch_prediction_bq_input_uri=actuals_table,
    batch_prediction_bq_output_uri=predictions_table,
    time_column=time_column,
    time_series_identifier_column=time_series_identifier_column,
    target_column=target_column,
)

print("Click the link below to view ARIMA predictions in Looker Studio:")
print(link)

"""
Looker Studio link:
https://lookerstudio.google.com/u/0/reporting?params=%7B%22templateId%22:%20%22067f70d2-8cd6-4a4c-a099-292acd1053e8%22,%20%22ds0.connector%22:%20%22BIG_QUERY%22,%20%22ds0.projectId%22:%20%22forecast-463716%22,%20%22ds0.billingProjectId%22:%20%22forecast-463716%22,%20%22ds0.type%22:%20%22CUSTOM_QUERY%22,%20%22ds0.sql%22:%20%22%5Cn%20%20%20%20%20%20%20%20SELECT%5Cn%20%20%20%20%20%20%20%20CAST(date%20AS%20DATETIME)%20AS%20timestamp_col,%5Cn%20%20%20%20%20%20%20%20CAST(id%20AS%20STRING)%20AS%20time_series_identifier_col,%5Cn%20%20%20%20%20%20%20%20CAST(sales%20AS%20NUMERIC)%20AS%20historical_values,%5Cn%20%20%20%20%20%20%20%20NULL%20AS%20predicted_values%5Cn%20%20%20%20%20%20%20%20FROM%20%60forecast-463716.forecasting_demo_arima.train%60%5Cn%5Cn%20%20%20%20%20%20%20%20UNION%20ALL%5Cn%5Cn%20%20%20%20%20%20%20%20SELECT%5Cn%20%20%20%20%20%20%20%20CAST(date%20AS%20DATETIME)%20AS%20timestamp_col,%5Cn%20%20%20%20%20%20%20%20CAST(id%20AS%20STRING)%20AS%20time_series_identifier_col,%5Cn%20%20%20%20%20%20%20%20NULL%20AS%20historical_values,%5Cn%20%20%20%20%20%20%20%20CAST(predicted_sales.value%20AS%20NUMERIC)%20AS%20predicted_values%5Cn%20%20%20%20%20%20%20%20FROM%20%60forecast-463716.forecasting_demo_arima.predictions_7422465586485002240%60%5Cn%20%20%20%20%22%7D
"""