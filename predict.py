import os
import uuid
from google.cloud import aiplatform
from google_cloud_pipeline_components.v1.automl.forecasting import utils
from setup import (
    PROJECT_ID,
    LOCATION,
    BUCKET_URI,
    SERVICE_ACCOUNT,
    PREDICTION_DATASET_BQ_PATH,
    arima_dataset_path
)

# Initialize Vertex AI SDK
aiplatform.init(project=PROJECT_ID, location=LOCATION)

# Use the ARIMA model name directly (from Vertex AI > Models)
model_name = ""

# Get the prediction job spec and parameters
predict_job_spec_path, predict_parameter_values = utils.get_bqml_arima_predict_pipeline_and_parameters(
    project=PROJECT_ID,
    location=LOCATION,
    model_name=model_name,
    data_source_bigquery_table_path=PREDICTION_DATASET_BQ_PATH,
    bigquery_destination_uri=arima_dataset_path,
)

# Generate a unique job name
PRED_DISPLAY_NAME = f"forecasting-demo-predict-{str(uuid.uuid1())}"

# Define and run the pipeline job
pred_job = aiplatform.PipelineJob(
    job_id=PRED_DISPLAY_NAME,
    display_name=PRED_DISPLAY_NAME,
    pipeline_root=os.path.join(BUCKET_URI, PRED_DISPLAY_NAME),
    template_path=predict_job_spec_path,
    parameter_values=predict_parameter_values,
    enable_caching=False,
)

# Run the prediction job
pred_job.run(service_account=SERVICE_ACCOUNT)
