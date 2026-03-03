import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text  # FIX 1: Explicitly import text for SQLAlchemy 2.0+

# ==========================================
# 1. Configuration & Alerting Callbacks
# ==========================================
POSTGRES_CONN_ID = 'telco_postgres_dw'
TARGET_SCHEMA = 'raw_staging'

# FIX 2: Updated paths to match your local Linux environment
DATA_DIR = '/home/guruparan/DataEngineer/'
DBT_PROJECT_DIR = '/home/guruparan/DataEngineer/telco_dwh_dbt'
VENV_ACTIVATE = 'source /home/guruparan/DataEngineer/myprojectenv/bin/activate'

def failure_alert(context):
    """
    Mock alerting mechanism. In production, this would use SlackWebhookOperator 
    or an EmailOperator to notify the data engineering team.
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    logging.error(f"🚨 ALERT: Task '{task_id}' failed! Check logs here: {log_url}")

default_args = {
    'owner': 'guruparan_de_team',
    'depends_on_past': False,
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': failure_alert, 
    'start_date': datetime(2026, 2, 28)
}

# ==========================================
# 2. Ingestion Logic 
# ==========================================
def ingest_data(table_name: str, file_name: str, load_method: str = 'replace', date_col: str = None):
    logger = logging.getLogger(__name__)
    file_path = os.path.join(DATA_DIR, file_name)
    
    try:
        df = pd.read_csv(file_path)
        df.replace({'': pd.NA, 'null': pd.NA, 'None': pd.NA}, inplace=True)
        
        # Ensure dates are timestamps for comparison
        for col in df.columns:
            if any(x in col.lower() for x in ['date', 'start', 'end', 'created']):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        with engine.begin() as connection:
            # Check if table exists first
            check_table = text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = '{table_name}');")
            table_exists = connection.execute(check_table).scalar()

            # --- Logic for Full Refresh (Replace) ---
            if load_method == 'replace' and table_exists:
                logger.info(f"Truncating {table_name} to preserve view dependencies.")
                connection.execute(text(f'TRUNCATE TABLE {TARGET_SCHEMA}."{table_name}" CASCADE;'))
                load_method = 'append' # Switch to append since table is now empty

            # --- Logic for Incremental (Append) ---
            elif load_method == 'append' and date_col and table_exists:
                raw_max_date = connection.execute(text(f'SELECT MAX("{date_col}") FROM {TARGET_SCHEMA}."{table_name}"')).scalar()
                if raw_max_date:
                    max_dt = pd.to_datetime(raw_max_date)
                    df = df[df[date_col] > max_dt]

            if df.empty:
                logger.info(f"No new data to load for {table_name}.")
                return

            # Final Load
            df.to_sql(
                name=table_name, 
                con=connection, 
                schema=TARGET_SCHEMA, 
                if_exists='append', # Always append after our manual truncate
                index=False, 
                method='multi', 
                chunksize=5000
            )
            logger.info(f"Successfully loaded {len(df)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Ingestion failed for {table_name}: {str(e)}")
        raise
# ==========================================
# 3. Data Quality Gate (Circuit Breaker)
# ==========================================
def check_staging_data_quality():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    critical_tables = ['stg_customers', 'stg_subscriptions', 'stg_billing_invoices']
    
    for table in critical_tables:
        # Airflow's internal get_first handles strings, but we explicitly use TARGET_SCHEMA
        records = pg_hook.get_first(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{table}")
        count = records[0] if records else 0
        if count == 0:
            logging.error(f"DQ Check Failed: Table {TARGET_SCHEMA}.{table} is empty!")
            return False 
            
    logging.info("DQ Check Passed: All critical staging tables have data.")
    return True

# ==========================================
# 4. DAG Definition & Dependencies
# ==========================================
with DAG(
    'telco_master_elt_pipeline',
    default_args=default_args,
    description='End-to-end Telco ELT: Ingest CSVs -> DQ Check -> dbt Run -> dbt Test',
    schedule='@daily',
    catchup=False,
    tags=['telco', 'elt', 'dbt', 'production']
) as dag:

    # --- Phase 1: Ingestion ---
    ingest_plans = PythonOperator(
        task_id='ingest_plans',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_service_plans', 'file_name': 'service_plans.csv', 'load_method': 'replace'}
    )
    ingest_customers = PythonOperator(
        task_id='ingest_customers',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_customers', 'file_name': 'customers.csv', 'load_method': 'replace'}
    )
    ingest_subs = PythonOperator(
        task_id='ingest_subscriptions',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_subscriptions', 'file_name': 'subscriptions.csv', 'load_method': 'replace'}
    )
    ingest_usage = PythonOperator(
        task_id='ingest_usage',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_network_usage', 'file_name': 'network_usage.csv', 'load_method': 'append', 'date_col': 'usage_date'}
    )
    ingest_billing = PythonOperator(
        task_id='ingest_billing',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_billing_invoices', 'file_name': 'billing_invoices.csv', 'load_method': 'append', 'date_col': 'billing_period_start'}
    )

    # --- Phase 2: Data Quality Gate ---
    dq_circuit_breaker = ShortCircuitOperator(
        task_id='dq_check_staging_row_counts',
        python_callable=check_staging_data_quality
    )

    # --- Phase 3: DBT Transformation ---
    # FIX 3: Activate local virtual environment before running dbt
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f"{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt run",
        retries=3, 
    )

    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command=f"{VENV_ACTIVATE} && cd {DBT_PROJECT_DIR} && dbt test",
    )

    # --- Task Dependencies ---
    ingestion_tasks = [ingest_plans, ingest_customers, ingest_subs, ingest_usage, ingest_billing]
    ingestion_tasks >> dq_circuit_breaker >> dbt_run >> dbt_test
