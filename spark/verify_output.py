# spark/verify_output.py
import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\risk-managemnt\json_key\terraform_service_account.json'

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('verify') \
    .master('local[*]') \
    .config('spark.jars.packages',
            'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11') \
    .config('fs.gs.impl',
            'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('fs.AbstractFileSystem.gs.impl',
            'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
    .config('google.cloud.auth.service.account.enable', 'true') \
    .config('google.cloud.auth.service.account.json.keyfile',
            r'C:\risk-managemnt\json_key\terraform_service_account.json') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.read.parquet(
    'gs://vertexai-489303-riskplatform-scored/features/'
)

total     = df.count()
high_risk = df.filter(df.high_risk_flag == 1).count()
low_risk  = df.filter(df.high_risk_flag == 0).count()

print(f"\n--- Feature Engineering Results ---")
print(f"Total accounts:     {total:,}")
print(f"High risk (flag=1): {high_risk:,} ({high_risk/total*100:.1f}%)")
print(f"Low risk  (flag=0): {low_risk:,}  ({low_risk/total*100:.1f}%)")
print(f"\nSample high risk accounts:")
df.filter(df.high_risk_flag == 1) \
  .select('account_id', 'credit_score', 'debt_ratio',
          'delinquency_risk_score', 'high_risk_flag') \
  .show(5)

spark.stop()