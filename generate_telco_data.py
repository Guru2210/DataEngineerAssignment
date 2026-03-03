import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

# ==========================================
# 1. Configuration & Setup
# ==========================================
fake = Faker()
Faker.seed(42)
np.random.seed(42)

NUM_CUSTOMERS = 10000
START_DATE = datetime(2025, 8, 1)
END_DATE = datetime(2026, 2, 1) # 6 Month period
MONTHS = pd.date_range(start=START_DATE, end=END_DATE, freq='MS')

print("Starting data generation process...")

# ==========================================
# 2. Generate Service Plans
# ==========================================
plans_data = [
    {'plan_id': 1, 'plan_name': 'Basic 5GB', 'monthly_fee': 20.00, 'infrastructure_cost': 5.00, 'data_limit_gb': 5},
    {'plan_id': 2, 'plan_name': 'Standard 20GB', 'monthly_fee': 45.00, 'infrastructure_cost': 12.00, 'data_limit_gb': 20},
    {'plan_id': 3, 'plan_name': 'Premium Unlimited', 'monthly_fee': 80.00, 'infrastructure_cost': 25.00, 'data_limit_gb': 999}
]
df_plans = pd.DataFrame(plans_data)

# ==========================================
# 3. Generate Customers
# ==========================================
print("Generating Customers...")
customer_segments = ['Student', 'Standard', 'High-Value', 'At-Risk']
segment_weights = [0.15, 0.50, 0.25, 0.10]

customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customers.append({
        'customer_id': i,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
        'gender': np.random.choice(['Male', 'Female', 'Non-Binary']),
        'customer_segment': np.random.choice(customer_segments, p=segment_weights),
        'created_at': fake.date_time_between(start_date='-2y', end_date=START_DATE)
    })
df_customers = pd.DataFrame(customers)

# ==========================================
# 4. Generate Subscriptions (Simulating SCD2)
# ==========================================
print("Generating Subscriptions & Churn Behavior...")
subscriptions = []
sub_id = 1

# Base churn probabilities per segment
churn_probs = {'Student': 0.15, 'Standard': 0.10, 'High-Value': 0.05, 'At-Risk': 0.35}
df_customers['is_churned'] = df_customers['customer_segment'].map(lambda x: np.random.random() < churn_probs[x])

for _, cust in df_customers.iterrows():
    c_id = cust['customer_id']

    # Assign initial plan based on segment
    if cust['customer_segment'] == 'High-Value':
        initial_plan = 3
    elif cust['customer_segment'] in ['Student', 'At-Risk']:
        initial_plan = 1
    else:
        initial_plan = np.random.choice([1, 2, 3], p=[0.3, 0.5, 0.2])

    # Simulate Plan Upgrades/Downgrades mid-lifecycle (for Question D)
    has_plan_change = np.random.random() < 0.15 # 15% of users change plans

    if has_plan_change and not cust['is_churned']:
        change_date = START_DATE + timedelta(days=np.random.randint(60, 120))
        # First subscription record (closed)
        subscriptions.append({
            'subscription_id': sub_id, 'customer_id': c_id, 'plan_id': initial_plan,
            'start_date': cust['created_at'].date(), 'end_date': change_date.date(), 'status': 'Upgraded/Downgraded'
        })
        sub_id += 1
        # Second subscription record (active)
        new_plan = np.random.choice([p for p in [1, 2, 3] if p != initial_plan])
        subscriptions.append({
            'subscription_id': sub_id, 'customer_id': c_id, 'plan_id': new_plan,
            'start_date': change_date.date(), 'end_date': None, 'status': 'Active'
        })
    else:
        # Single subscription record
        end_d = START_DATE + timedelta(days=np.random.randint(30, 170)) if cust['is_churned'] else None
        status = 'Cancelled' if cust['is_churned'] else 'Active'
        subscriptions.append({
            'subscription_id': sub_id, 'customer_id': c_id, 'plan_id': initial_plan,
            'start_date': cust['created_at'].date(), 'end_date': end_d.date() if end_d else None, 'status': status
        })
    sub_id += 1

df_subscriptions = pd.DataFrame(subscriptions)

# ==========================================
# 5. Generate Network Usage (Vectorized)
# ==========================================
print("Generating Network Usage Telemetry...")
# Generating daily records for 10k users over 180 days is ~1.8M rows.
# We will use a representative sample (weekly grain) for the script to run efficiently,
# simulating daily rollups.
weeks = pd.date_range(start=START_DATE, end=END_DATE, freq='W')
usage_records = []

# Merge customer and plan data to inform usage logic
active_subs = df_subscriptions[df_subscriptions['status'] == 'Active'].merge(df_plans, on='plan_id')

for week in weeks:
    # Introduce anomalies: At-Risk users have higher dropped calls
    base_dropped = np.where(active_subs['customer_id'].isin(df_customers[df_customers['customer_segment'] == 'At-Risk']['customer_id']), 5, 1)

    usage_df = pd.DataFrame({
        'customer_id': active_subs['customer_id'],
        'usage_date': week.date(),
        # Add random noise to data consumption based on plan limits
        'data_consumed_mb': np.random.normal(loc=(active_subs['data_limit_gb'] * 1024 * 0.2), scale=500, size=len(active_subs)).clip(min=0),
        'dropped_calls_count': np.random.poisson(lam=base_dropped),
        'average_latency_ms': np.random.normal(loc=45, scale=15, size=len(active_subs)).clip(min=10)
    })
    usage_records.append(usage_df)

df_usage = pd.concat(usage_records, ignore_index=True)
df_usage.insert(0, 'usage_id', range(1, len(df_usage) + 1))

# ==========================================
# 6. Generate Billing Invoices
# ==========================================
print("Generating Billing Invoices...")
invoices = []
inv_id = 1

for month_start in MONTHS:
    month_end = month_start + pd.offsets.MonthEnd(1)

    # Find active subscriptions for this month
    valid_subs = df_subscriptions[
        (pd.to_datetime(df_subscriptions['start_date']) <= month_end) &
        ((pd.isnull(df_subscriptions['end_date'])) | (pd.to_datetime(df_subscriptions['end_date']) >= month_start))
    ].merge(df_plans, on='plan_id')

    for _, sub in valid_subs.iterrows():
        # Inject business logic: Students and At-Risk have higher delay probability
        cust_segment = df_customers.loc[df_customers['customer_id'] == sub['customer_id'], 'customer_segment'].values[0]
        delay_prob = 0.25 if cust_segment in ['Student', 'At-Risk'] else 0.05
        is_delayed = np.random.random() < delay_prob

        invoices.append({
            'invoice_id': inv_id,
            'customer_id': sub['customer_id'],
            'subscription_id': sub['subscription_id'],
            'billing_period_start': month_start.date(),
            'billing_period_end': month_end.date(),
            'amount_due': sub['monthly_fee'],
            'due_date': (month_end + timedelta(days=15)).date(),
            'payment_date': (month_end + timedelta(days=25)).date() if is_delayed else (month_end + timedelta(days=random.randint(5, 14))).date(),
            'is_delayed': is_delayed
        })
        inv_id += 1

df_billing = pd.DataFrame(invoices)

# ==========================================
# 7. Exporting to CSV (or PostgreSQL)
# ==========================================
print("Exporting data to CSV files...")
df_plans.to_csv('service_plans.csv', index=False)
df_customers.to_csv('customers.csv', index=False)
df_subscriptions.to_csv('subscriptions.csv', index=False)
df_usage.to_csv('network_usage.csv', index=False)
df_billing.to_csv('billing_invoices.csv', index=False)

# Optional: To load directly to PostgreSQL via SQLAlchemy (Requires psycopg2)
"""
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:password@localhost:5432/telco_db')
df_customers.to_sql('customer', engine, if_exists='replace', index=False)
# Repeat for other dataframes...
"""

print("Data generation complete! Synthetic datasets are ready.")