import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random
import string

def generate_account_id(n):
    return [
        ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        for _ in range(n)
    ]

def generate_synthetic_accounts(n_records=50000, seed=42):
    np.random.seed(seed)
    random.seed(seed)

    print(f"Generating {n_records} synthetic account records...")

    # Account identifiers
    account_ids = generate_account_id(n_records)

    # Credit scores — roughly normal distribution 300-850
    credit_scores = np.clip(
        np.random.normal(loc=650, scale=100, size=n_records),
        300, 850
    ).astype(int)

    # Monthly income — log-normal distribution
    monthly_income = np.random.lognormal(
        mean=8.5, sigma=0.6, size=n_records
    ).astype(int)

    # Account balance — correlated with income
    balance = (
        monthly_income * np.random.uniform(0.5, 8.0, size=n_records)
    ).astype(int)

    # Debt to income ratio
    debt_ratio = np.clip(
        np.random.beta(2, 5, size=n_records),
        0.01, 0.99
    ).round(4)

    # Delinquency days in last 30/60/90 days
    delinquency_30 = np.random.poisson(lam=0.3, size=n_records)
    delinquency_60 = np.random.poisson(lam=0.15, size=n_records)
    delinquency_90 = np.random.poisson(lam=0.08, size=n_records)

    # Number of open credit lines
    open_credit_lines = np.random.poisson(lam=8, size=n_records)

    # Number of dependents
    dependents = np.random.choice(
        [0, 1, 2, 3, 4, 5],
        p=[0.35, 0.25, 0.20, 0.12, 0.05, 0.03],
        size=n_records
    )

    # Age
    age = np.random.randint(18, 80, size=n_records)

    # Number of times 90 days late
    times_90_days_late = np.random.poisson(lam=0.1, size=n_records)

    # Real estate loans
    real_estate_loans = np.random.poisson(lam=1.0, size=n_records)

    # Default label — derived from risk factors
    default_prob = (
        0.05 +
        0.15 * (credit_scores < 580).astype(float) +
        0.10 * (debt_ratio > 0.5).astype(float) +
        0.08 * (delinquency_90 > 0).astype(float) +
        0.05 * (times_90_days_late > 0).astype(float)
    )
    default_prob = np.clip(default_prob, 0, 1)
    serious_delinquency = np.random.binomial(1, default_prob)

    # Account open date
    base_date = datetime(2020, 1, 1)
    account_open_dates = [
        (base_date + timedelta(days=random.randint(0, 1460))).strftime('%Y-%m-%d')
        for _ in range(n_records)
    ]

    df = pd.DataFrame({
        'account_id':             account_ids,
        'credit_score':           credit_scores,
        'monthly_income':         monthly_income,
        'balance':                balance,
        'debt_ratio':             debt_ratio,
        'delinquency_30_days':    delinquency_30,
        'delinquency_60_days':    delinquency_60,
        'delinquency_90_days':    delinquency_90,
        'open_credit_lines':      open_credit_lines,
        'dependents':             dependents,
        'age':                    age,
        'times_90_days_late':     times_90_days_late,
        'real_estate_loans':      real_estate_loans,
        'serious_delinquency':    serious_delinquency,
        'account_open_date':      account_open_dates,
    })

    return df

def generate_credit_bureau(account_ids, seed=42):
    np.random.seed(seed)

    print(f"Generating credit bureau data for {len(account_ids)} accounts...")

    df = pd.DataFrame({
        'account_id':               account_ids,
        'bureau_score':             np.clip(
                                        np.random.normal(650, 90, len(account_ids)),
                                        300, 850
                                    ).astype(int),
        'inquiries_last_6mo':       np.random.poisson(1.5, len(account_ids)),
        'collections_last_12mo':    np.random.poisson(0.2, len(account_ids)),
        'public_records':           np.random.poisson(0.1, len(account_ids)),
        'revolving_utilization':    np.clip(
                                        np.random.beta(2, 4, len(account_ids)),
                                        0, 1
                                    ).round(4),
        'bureau_updated_date':      pd.Timestamp.now().strftime('%Y-%m-%d'),
    })

    return df

if __name__ == "__main__":
    output_dir = os.path.join(os.path.dirname(__file__), 'synthetic')
    os.makedirs(output_dir, exist_ok=True)

    # Generate accounts
    accounts_df = generate_synthetic_accounts(n_records=50000)
    accounts_path = os.path.join(output_dir, 'accounts.parquet')
    accounts_df.to_parquet(accounts_path, index=False)
    print(f"Saved {len(accounts_df)} accounts to {accounts_path}")

    # Generate credit bureau data
    bureau_df = generate_credit_bureau(accounts_df['account_id'].tolist())
    bureau_path = os.path.join(output_dir, 'credit_bureau.parquet')
    bureau_df.to_parquet(bureau_path, index=False)
    print(f"Saved {len(bureau_df)} bureau records to {bureau_path}")

    # Also save as CSV for easy inspection
    accounts_df.head(1000).to_csv(
        os.path.join(output_dir, 'accounts_sample.csv'), index=False
    )

    print("\nSample record:")
    print(accounts_df.head(2).to_string())
    print(f"\nDefault rate: {accounts_df['serious_delinquency'].mean():.2%}")
    print(f"Avg credit score: {accounts_df['credit_score'].mean():.0f}")