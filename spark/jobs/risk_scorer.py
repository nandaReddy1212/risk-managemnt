import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F          
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'

os.environ['HADOOP_HOME']  = r'C:\hadoop'

# --- environment variables ---
PROJECT_ID    = os.environ.get('GCP_PROJECT_ID',  'vertexai-489303')
SCORED_BUCKET = os.environ.get('SCORED_BUCKET',   'gs://vertexai-489303-riskplatform-scored')
BQ_TABLE      = os.environ.get('BQ_TABLE',        'vertexai-489303.risk_results.risk_scored_accounts')
CREDS_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', r'C:\risk-managemnt\json_key\terraform_service_account.json')

# --- label and threshold constants ---
LABEL_COL        = 'serious_delinquency'
LOW_THRESHOLD    = 0.3
MEDIUM_THRESHOLD = 0.6
MODEL_VERSION    = '1.0.0'

# --- feature columns ---

FEATURE_COLS = [
    'credit_score',
    'debt_ratio',
    'monthly_income',
    'delinquency_risk_score',
    'debt_stress',
    'bureau_score',
    'revolving_utilization',
    'inquiries_last_6mo'
    # you write the remaining 7 columns
]

def create_spark_session(app_name='risk_score'):
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
    scored_path = f'{SCORED_BUCKET}/features/'
    print(f"Reading scored features from: {scored_path}")
    scored_df = spark.read.format("parquet").load(scored_path)
    print(f"Features loaded: {scored_df.count()} rows")
    return scored_df

def prepare_features(scored_df):
    """
    Assembles individual feature columns into a single vector
    column required by PySpark ML models.
    
    Business reason: ML models need all input signals packaged
    as one vector - VectorAssembler does this transformation.
    
    Also casts label column to DoubleType because PySpark ML
    requires numeric labels to be doubles not integers.
    """
    # Step 1 - handle any nulls in feature columns
    # fill with 0 so model doesn't break on missing data
    scored_df = scored_df.fillna(0, subset=FEATURE_COLS)

    # Step 2 - assemble features into 'features' vector column
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol='features')

    scored_df= assembler.transform(scored_df)

    # Step 3 - cast label to DoubleType (PySpark ML requirement)
    scored_df = scored_df.withColumn(LABEL_COL, F.col(LABEL_COL).cast(DoubleType()))
    return scored_df

def train_model(scored_df):
    """
    Trains a logistic regression model to predict serious delinquency.
    Business reason: Model learns the relationship between
    customer features and actual default history so it can
    predict risk for new credit card applicants.

    Train/test split ensures we measure real performance
    on data the model has never seen.
    """
    # Step 1 - split data 80% train, 20% test
    train_df, test_df = scored_df.randomSplit([0.8, 0.2], seed=42)

    print(f"Training set: {train_df.count()} rows")
    print(f"Test set:     {test_df.count()} rows")

    # Step 2 - train logistic regression model
    # labelCol is the target variable, featuresCol is the vector of input features
    # maxIter controls how long the model trains - more iterations can improve performance but take longer
    lr = LogisticRegression(labelCol=LABEL_COL, featuresCol='features', maxIter=10)

    # Step 3 - train the model 
    model = lr.fit(train_df)

    # Step 4 = evaluate model on test set
    predictions = model.transform(test_df)

    # Step 5 - evaluate using AUC metric

    evaluator = BinaryClassificationEvaluator(labelCol=LABEL_COL, metricName='areaUnderROC')
    auc = evaluator.evaluate(predictions)
    print(f"Model AUC score: {auc:.4f}")
    print(f"Interpretation: {'Good' if auc > 0.7 else 'Needs improvement'}")

    return model

def score_accounts(model, scored_df):
    """
    Uses the trained model to predict risk scores for all accounts.
    Business reason: We want to apply the model to all customers
    to identify who is high risk and needs attention.

    The output includes the original features, the predicted probability
    of delinquency, and a risk category based on defined thresholds.
    """
    # Step 1 - generate predictions (probability of delinquency)
    scored_df = model.transform(scored_df)

    # Step 2 - extract probability of positive class (delinquency)
    scored_df = scored_df.withColumn('risk_score', vector_to_array(F.col('probability'))[1])

    # Step 3 - categorize risk based on thresholds
    scored_df = scored_df.withColumn('risk_band',
        F.when(F.col('risk_score') <= LOW_THRESHOLD, 'LOW')
         .when((F.col('risk_score') > LOW_THRESHOLD) & (F.col('risk_score') <= MEDIUM_THRESHOLD), 'MEDIUM')
         .otherwise('HIGH')
    )

    # Step 4 - add_Metadata
    scored_df = scored_df.withColumn('model_version', F.lit(MODEL_VERSION))
    scored_df = scored_df.withColumn('scored_at', F.current_timestamp())

    return scored_df

def write_to_bigquery(scored_df):
    """
    Writes final risk scores to BigQuery.
    Selects and renames columns to match exact table schema.
    
    Business reason: Downstream teams query BigQuery to get
    risk decisions. Column names must match table schema exactly.
    """
    # Step 1 - select and rename columns to match BQ table schema
    final_df = scored_df.select(
        F.col('account_id'),
        F.col('risk_score').alias('score'),        # rename to match table
        F.col('risk_band'),
        F.col('scored_at'),
        F.col('model_version'),
        F.col('delinquency_risk_score'),
        F.col('debt_stress').alias('debt_stress_score'), # rename to match table
        F.col('high_risk_flag'),
        F.col('credit_score'),
        F.col('debt_ratio'),
        F.col('monthly_income')
    )

    print(f"Writing {final_df.count()} accounts to BigQuery...")

    final_df.write.format('bigquery') \
        .option('table', BQ_TABLE) \
        .option('temporaryGcsBucket', 'vertexai-489303-riskplatform-models') \
        .mode('overwrite') \
        .save()
    print(f"Successfully written to: {BQ_TABLE}")

def main():
    spark = create_spark_session()
    scored_df = read_data(spark)
    prepared_df = prepare_features(scored_df)
    model = train_model(prepared_df)
    scored_accounts_df = score_accounts(model, prepared_df)
    write_to_bigquery(scored_accounts_df)
    spark.stop()
    print("Risk scoring job completed successfully")

if __name__ == "__main__":
    main()