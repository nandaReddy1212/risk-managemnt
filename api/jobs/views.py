import logging
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.utils import timezone

from .models import SparkJob
from .serializer import SparkJobSerializer, SparkJobDetailSerializer
from .tasks import submit_spark_job

logger = logging.getLogger(__name__)

class SparkJobViewSet(viewsets.ModelViewSet):
    """
    ViewSet for managing Spark jobs.
    
    Provides:
        GET    /api/jobs/         - list all jobs for current user
        POST   /api/jobs/         - create and submit new job
        GET    /api/jobs/{id}/    - get job details
        POST   /api/jobs/{id}/retry/ - retry failed job
    
    Business reason: Statisticians submit and monitor Spark jobs
    through this API without needing GCP console access.
    """
    permission_classes = [IsAuthenticated]  # only logged-in users can access
    serializer_class  = SparkJobSerializer

    def get_queryset(self):
        """
        Returns jobs for current user only.
        Admins see all jobs.
        
        Business reason: Statisticians should only see their
        own jobs not colleagues private work.
        """
        user = self.request.user
        if user.is_staff:
            return SparkJob.objects.all()
        return SparkJob.objects.filter(submitted_by=user)
    
    def get_serializer_class(self):
        """
        Use detailed serializer for admin users.
        
        Business reason: Admins need more internal details for
        troubleshooting, while regular users get a cleaner view.
        """
        if self.request.user.is_staff:
            return SparkJobDetailSerializer
        return SparkJobSerializer
    
    def create(self, request):
        """
        Creates a new Spark job and submits it to Dataproc.
        Returns job_id immediately - processing happens async.
        """
        # step 1 - validate input data
        serializer = SparkJobSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        # step 2 - save job to DB with PENDING status
        job = serializer.save(submitted_by=request.user, status="PENDING")
        
        # step 3 - send to celery for async processing
        submit_spark_job.delay(job.job_id)

        # step 4 - return job_id to user immediately
        return Response(
            {
                'job_id':   job.job_id,
                'status':   job.status,
                'message':  'Job submitted successfully'
            },
            status=status.HTTP_201_CREATED
        )
    
    @action(detail=True, methods=['post'])
    def retry(self, request, pk=None):
        """
        Retries a failed job.
        
        Business reason: Jobs can fail due to temporary GCP
        issues. Retry lets statisticians rerun without
        resubmitting from scratch.
        """
        job = self.get_object()

        # only allow retry if job failed
        if job.status != 'FAILED':
            return Response(
                {
                    'error': 'Only failed jobs can be retried.'
                },
                status=status.HTTP_400_BAD_REQUEST
            
            )
    
        # reset job status and timestamps
        job.status = 'PENDING'
        job.started_at = None
        job.completed_at = None
        job.error_message = None
        job.save()

        # resubmit job to Celery
        submit_spark_job.delay(job.job_id)

        return Response(
            {'message': f'Job {job.job_id} resubmitted'},
            status=status.HTTP_200_OK
        )
    

    @action(detail=True, methods=['get'])
    def status_check(self, request, pk=None):
        """
        Returns current status of a specific job.
        Statisticians poll this endpoint to track progress.
        """
        job        = self.get_object()
        serializer = self.get_serializer(job)
        return Response(serializer.data)


    