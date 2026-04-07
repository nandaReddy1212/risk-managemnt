import logging
from celery import shared_task
from django.utils import timezone
from google.cloud import dataproc_v1

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3)
def submit_spark_job(self, job_id):
    """
    Celery task that submits a Spark job to Dataproc Serverless.
    
    Business reason: Runs asynchronously so API response is
    instant. Statistician gets job_id immediately while
    Dataproc processes in background.
    
    bind=True    - gives access to task instance (self)
    max_retries=3 - retries 3 times if Dataproc submission fails
    """
    from django.conf import settings
    from .models import SparkJob

    #step 1 - fetch job details from DB
    try:
        job = SparkJob.objects.get(job_id=job_id)
    except SparkJob.DoesNotExist:
        logger.error(f"SparkJob with ID {job_id} does not exist.")
        return
    try:
        #step 2 - update job status to RUNNING and set started_at
        job.status = "RUNNING"
        job.started_at = timezone.now()
        job.save()

        #step 3 - build GCS Path for job output
        job_script = f"{settings.GCS_RAW_BUCKET}/jobs/{job.job_type}.py"

        #step 4 - submit job to Dataproc Serverless
        client = dataproc_v1.BatchControllerClient(
            client_options={
                "api_endpoint": f"{settings.DATAPROC_REGION}-dataproc.googleapis.com"
            }
        )

        batch = dataproc_v1.Batch()
        batch.pyspark_batch = dataproc_v1.PySparkBatch()
        batch.pyspark_batch.main_python_file_uri = job_script

        request = dataproc_v1.CreateBatchRequest(
            parent=f"projects/{settings.GCP_PROJECT_ID}/locations/{settings.DATAPROC_REGION}",
            batch=batch,
        )

        operation = client.create_batch(request=request)
        result = operation.result()

        # step 5 - update job with Dataproc batch ID and  Status to COMPLETED
        job.dataproc_job_id = result.batch_id
        job.status = "COMPLETED"
        job.completed_at = timezone.now()
        job.save()

        logger.info(f"Job {job_id} completed successfully")
    except Exception as exc:
        logger.error(f"Error processing job {job_id}: {exc}")
        # update job status to FAILED and save error message
        job.status = "FAILED"
        job.error_message = str(exc)
        job.completed_at = timezone.now()
        job.save()
        # retry the task with exponential backoff
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)