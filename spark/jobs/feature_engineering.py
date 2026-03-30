# =============================================================
# feature_engineering.py
#
# Business Purpose:
#   Analyze customer credit history to identify risk signals
#   before issuing credit cards. Combines raw account data
#   with credit bureau data to engineer features for the
#   risk scoring model.
#

# INPUTS FORM RAW DATA IN GCS
# OUTPUT TO SCORED FEATURES IN GCS
#======================================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F          
from pyspark.sql.types import DoubleType, IntegerType

os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'

os.environ['HADOOP_HOME']  = r'C:\hadoop'

# --- environment variables ---
PROJECT_ID    = os.environ.get('GCP_PROJECT_ID',  
                os.environ.get('SPARK_ENV_GCP_PROJECT_ID', 'vertexai-489303'))
RAW_BUCKET    = os.environ.get('RAW_BUCKET',       
                os.environ.get('SPARK_ENV_RAW_BUCKET', 'gs://vertexai-489303-riskplatform-raw'))
SCORED_BUCKET = os.environ.get('SCORED_BUCKET',    
                os.environ.get('SPARK_ENV_SCORED_BUCKET', 'gs://vertexai-489303-riskplatform-scored'))
CREDS_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', r'C:\risk-managemnt\json_key\terraform_service_account.json')

# ---- INPUT/OUTPUT PATHS ----

ACCOUNTS_PATH = f'{RAW_BUCKET}/synthetic/accounts.parquet'
BUREAU_PATH = f'{RAW_BUCKET}/synthetic/credit_bureau.parquet'
FEATURES_OUTPUT_PATH = f'{SCORED_BUCKET}/features/'

def create_spark_session(app_name='riskplatform-feature-engineering'):
    """
    Creates SparkSession configured for GCS access.
    
    Business reason: Spark needs GCS connector jar to read/write
    gs:// paths. Without this, Spark only reads local files.
    
    master() is set via environment variable so same code runs
    locally (local[*]) and on Dataproc/GKE (yarn or k8s).
    """
    master = os.environ.get('SPARK_MASTER', 'local[*]')
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark

def read_data(spark):
    """
    Reads raw account and bureau data from GCS.
    Prints row counts so we know data landed correctly.
    """
    print(f"Reading accounts from: {ACCOUNTS_PATH}")
    accounts_df = spark.read.parquet(ACCOUNTS_PATH)

    print(f"Reading bureau data from: {BUREAU_PATH}")
    bureau_df = spark.read.parquet(BUREAU_PATH)

    print(f"Accounts loaded:  {accounts_df.count()} rows")
    print(f"Bureau loaded:    {bureau_df.count()} rows")

    return accounts_df, bureau_df


def engineer_features(accounts_df, bureau_df):
    """
    Joins accounts with bureau data and engineers risk features.

    Business logic:
        - Left join on account_id keeps all accounts
          even if bureau data is missing
        - delinquency_risk_score accumulates all delinquency signals
          90 days = 3pts, 60 days = 2pts, 30 days = 1pt
        - debt_stress flags financially stretched customers
        - high_risk_flag is the final binary risk indicator
    """
    # Step 1 - left join on account_id
    df = accounts_df.join(bureau_df, on='account_id', how='left')

    # Step 2 - fill nulls from missing bureau records
    df = df.fillna({
        'bureau_score':          0,
        'revolving_utilization': 0.0,
        'inquiries_last_6mo':    0
    })

    # Step 3 - delinquency risk score (additive - captures all signals)
    df = df.withColumn('delinquency_risk_score',
        (F.col('delinquency_90_days') * 3) +
        (F.col('delinquency_60_days') * 2) +
        (F.col('delinquency_30_days') * 1)
    )

    # Step 4 - debt stress indicator
    # high debt ratio AND low income = financially stressed
    df = df.withColumn('debt_stress',
        F.when(
            (F.col('debt_ratio') > 0.5) &        # fixed parentheses
            (F.col('monthly_income') < 5000),
            1
        ).otherwise(0)
    )

    # Step 5 - high risk flag
    # any one of these signals = flag the account
    df = df.withColumn('high_risk_flag',
        F.when(
            (F.col('credit_score') < 580) |           # fixed parentheses
            (F.col('delinquency_risk_score') >= 3) |  # >= catches accumulated scores
            (F.col('debt_ratio') > 0.5),              # fixed typo debt_ration -> debt_ratio
            1
        ).otherwise(0)
    )

    return df

def write_features(df):
    """
    Writes engineered features to GCS scored bucket.
    
    Business reason: (you write this - why do we partition?)
    
    We partition by high_risk_flag so downstream jobs can read
    only high risk accounts without scanning all data.
    """
    print(f"Writing features to: {FEATURES_OUTPUT_PATH}")
    df.write.mode('overwrite').partitionBy('high_risk_flag').parquet(FEATURES_OUTPUT_PATH)
    print("Features written successfully.")

def main():
    """
    Orchestrates the full feature engineering pipeline.
    This is what runs when the job is submitted to Dataproc or GKE.
    """
    print("Starting feature engineering job...")

    spark = create_spark_session(app_name='riskplatform-feature-engineering')
    accounts_df, bureau_df = read_data(spark)
    df = engineer_features(accounts_df, bureau_df)

    # print risk summary before writing
    total_accounts = df.count()
    high_risk_account = df.filter(F.col('high_risk_flag') == 1).count()
    print(f"Total accounts processed: {total_accounts}")
    print(f"High risk accounts: {high_risk_account} ({(high_risk_account/total_accounts)*100:.2f}%)")


    write_features(df)
    spark.stop()

    print("Feature engineering job complete.")

if __name__ == "__main__":
    main()
