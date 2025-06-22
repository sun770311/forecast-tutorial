from google.cloud import aiplatform
import uuid
import os
from config_arima import (
    train_job_spec_path,
    train_parameter_values,
)
from setup import BUCKET_URI, SERVICE_ACCOUNT

aiplatform.init(
    project="",
    location="us-central1",
    staging_bucket=BUCKET_URI
)


TRAIN_DISPLAY_NAME = f"forecasting-demo-train-{str(uuid.uuid1())}"

job = aiplatform.PipelineJob(
    job_id=TRAIN_DISPLAY_NAME,
    display_name=TRAIN_DISPLAY_NAME,
    pipeline_root=os.path.join(BUCKET_URI, TRAIN_DISPLAY_NAME),
    template_path=train_job_spec_path,
    parameter_values=train_parameter_values,
    enable_caching=False,
)

job.run(service_account=SERVICE_ACCOUNT) # in future, run async with sync=False
