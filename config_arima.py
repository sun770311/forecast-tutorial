from google_cloud_pipeline_components.v1.automl.forecasting import utils
import uuid
import os
from setup import TRAINING_DATASET_BQ_PATH, arima_dataset_path, BUCKET_URI, PROJECT_ID, LOCATION, SERVICE_ACCOUNT

# ARIMA config
time_column = "date"
time_series_identifier_column = "id"
target_column = "sales"
forecast_horizon = 30
data_granularity_unit = "day"
split_column = "split"
window_stride_length = 1
max_order = 3
override_destination = True

(
    train_job_spec_path,
    train_parameter_values,
) = utils.get_bqml_arima_train_pipeline_and_parameters(
    project=PROJECT_ID,
    location=LOCATION,
    root_dir=os.path.join(BUCKET_URI, "pipeline_root"),
    time_column=time_column,
    time_series_identifier_column=time_series_identifier_column,
    target_column=target_column,
    forecast_horizon=forecast_horizon,
    data_granularity_unit=data_granularity_unit,
    predefined_split_key=split_column,
    data_source_bigquery_table_path=TRAINING_DATASET_BQ_PATH,
    window_stride_length=window_stride_length,
    bigquery_destination_uri=arima_dataset_path,
    override_destination=override_destination,
    max_order=max_order,
)
