# submit_job.ps1
# Submits PySpark jobs to Dataproc Serverless
# Usage: .\spark\submit_job.ps1 -job feature_engineering

param(
    [string]$job = "feature_engineering"
)

$PROJECT_ID    = "vertexai-489303"
$REGION        = "us-central1"
$RAW_BUCKET    = "gs://vertexai-489303-riskplatform-raw"
$SCORED_BUCKET = "gs://vertexai-489303-riskplatform-scored"
$JOBS_BUCKET   = "$RAW_BUCKET/jobs"

Write-Host "Submitting $job to Dataproc Serverless..." -ForegroundColor Green

# Upload latest job file to GCS first
Write-Host "Uploading latest job code to GCS..."

gsutil cp spark/jobs/$job.py $JOBS_BUCKET/$job.py

# Submit the job to Dataproc Serverless
# gcloud dataproc batches submit  `
#     pyspark $JOBS_BUCKET/$job.py `
#     --project=$PROJECT_ID `
#     --region=$REGION `
#     --deps-bucket=$RAW_BUCKET `
#     --subnet=default `
#     --properties="spark.executorEnv.GCP_PROJECT_ID=$PROJECT_ID,spark.executorEnv.RAW_BUCKET=$RAW_BUCKET,spark.executorEnv.SCORED_BUCKET=$SCORED_BUCKET" `
#     --labels="job=$job,env=dev"
# Simplified submit - no env vars needed, defaults in code handle it
gcloud dataproc batches submit pyspark `
    $JOBS_BUCKET/$job.py `
    --project=$PROJECT_ID `
    --region=$REGION `
    --deps-bucket=$RAW_BUCKET `
    --subnet=default `
    --labels="job=$job,env=dev"

Write-Host "Job submitted! Check status at:"
Write-Host "https://console.cloud.google.com/dataproc/batches?project=$PROJECT_ID"
