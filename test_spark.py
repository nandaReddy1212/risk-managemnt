# test_spark.py
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\risk-managemnt\json_key\terraform_service_account.json'
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('test') \
    .master('local[*]') \
    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11') \
    .config('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
    .config('google.cloud.auth.service.account.enable', 'true') \
    .config('google.cloud.auth.service.account.json.keyfile', 
            r'C:\risk-managemnt\json_key\terraform_service_account.json') \
    .getOrCreate()

print(f'Spark {spark.version} working!')

# Read from GCS
df = spark.read.parquet(
    'gs://vertexai-489303-riskplatform-raw/synthetic/accounts.parquet'
)

print(f'Row count: {df.count()}')
print('\nSchema:')
df.printSchema()
print('\nSample rows:')
df.show(5)

spark.stop()