from rest_framework import serializers
from .models import SparkJob


class SparkJobSerializer(serializers.ModelSerializer):
    """
    Serializer for SparkJob model.
    Controls which fields are exposed in the API response.
    
    Business reason: Statisticians need to see job status and
    output path. Internal fields like dataproc_batch_id are
    hidden from regular users.
    """
    # this adds submitted_by username instead of just user ID
    submitted_by = serializers.StringRelatedField(source='submitted_by.username', read_only=True)

    class Meta:
        model = SparkJob
        # explicitly list fields to include in API response
        fields = [
            'job_id',
            'job_name',
            'job_type',
            'submitted_by',
            'status',
            'created_at',
            'started_at',
            'completed_at',
            'gcs_output_path',
        ]
        # exclude internal fields like error_message and dataproc_job_id
        read_only_fields = ['job_id', 'created_at', 'started_at', 'completed_at']  # these fields are read-only

class SparkJobDetailSerializer(SparkJobSerializer):
    """
    Extended serializer for admin users.
    Shows additional internal fields hidden from regular users.
    """
    class Meta(SparkJobSerializer.Meta):
        fields = SparkJobSerializer.Meta.fields + [
            'error_message',
            'dataproc_job_id',    # changed from dataproc_batch_id to match model
        ]