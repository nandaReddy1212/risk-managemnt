from django.db import models
from django.contrib.auth.models import User


class SparkJob(models.Model):

    # status choices
    # business reason: restricts status to valid values only
    STATUS_CHOICES = [
        ('PENDING',   'Pending'),
        ('RUNNING',   'Running'),
        ('COMPLETED', 'Completed'),
        ('FAILED',    'Failed'),
    ]

    JOB_TYPE_CHOICES = [
        ('feature_engineering', 'Feature Engineering'),
        ('risk_scorer',         'Risk Scorer'),
        ('batch_pipeline',      'Batch Pipeline'),
    ]

    # you write all the fields here
    # use the column list above and the field type examples
    job_id = models.AutoField(primary_key=True)  # auto-incrementing ID
    job_name   = models.CharField(max_length=200)
    job_type = models.CharField(max_length=50, choices=JOB_TYPE_CHOICES)
    submitted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)  # link to Django's User model
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="PENDING")
    created_at = models.DateTimeField(auto_now_add=True)  # add this field
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    gcs_output_path = models.CharField(max_length=500, null=True, blank=True)  # GCS path for job output
    error_message = models.TextField(null=True, blank=True)  # store error details if job fails
    dataproc_job_id = models.CharField(max_length=100, null=True, blank=True)  # store Dataproc job ID for tracking

    # you write the rest...

    class Meta:
        ordering = ['-created_at']  # newest jobs first
        db_table = 'spark_jobs'     # exact table name in PostgreSQL

    def __str__(self):
        return f"{self.job_name} - {self.status}"