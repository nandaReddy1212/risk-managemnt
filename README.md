# risk-managemnt

# STEP 1
"I built a pipeline in PySpark that reads raw credit account data and bureau data from GCS, indicates risk signals including a weighted delinquency score and debt stress indicator, flags high risk accounts, and writes partitioned Parquet output to GCS. The job runs on Dataproc Serverless and I verified the output with a 12% high risk rate which matches the synthetic data distribution we designed."


# SPARKML 

# VectorAssembler 
    — this is Step 2 — it packages multiple columns into one vector column that ML models understand
# LogisticRegression 
    — this is Step 3 — the actual model
# BinaryClassificationEvaluator 
    — measures how good the model is — gives you an AUC score between 0 and 1 where 1 is perfect

# STEP 2 
risk_scorer.py — reads engineered features, assembles ML feature vectors, trains a logistic regression model with 80/20 train/test split, evaluates with AUC, scores all 50k accounts with default probability, assigns LOW/MEDIUM/HIGH risk bands, writes to BigQuery partitioned by day.