import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text  # Add 'text' here

# ==========================================
# 1. Configuration & Default Arguments
# ==========================================
POSTGRES_CONN_ID = 'telco_postgres_dw' # Defined in Airflow UI
TARGET_SCHEMA = 'raw_staging'
DATA_DIR = '/home/guruparan/DataEngineer' # Path where the Part 2 CSVs are stored

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 2, 28)
}

# ==========================================
# 2. Core Ingestion Logic
# ==========================================
def ingest_data(table_name: str, file_name: str, load_method: str = 'replace', date_col: str = None):
    """
    Reads a CSV, performs basic cleansing, and loads it into the staging schema.
    Supports 'replace' (full load) and 'append' (incremental load) methods.
    """
    logger = logging.getLogger(__name__)
    file_path = os.path.join(DATA_DIR, file_name)

    try:
        logger.info(f"Starting ingestion for {table_name} from {file_path}")

        # 1. Extract
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        df = pd.read_csv(file_path)

        # 2. Basic Cleansing & Type Casting
        # Handle implicit nulls (e.g., empty strings to actual NaNs)
        df.replace({'': pd.NA, 'null': pd.NA, 'None': pd.NA}, inplace=True)

        # Dynamically cast common date columns to datetime objects
        for col in df.columns:
            if 'date' in col.lower() or 'created_at' in col.lower():
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 3. Load to PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()

        with engine.begin() as connection:
            # Incremental Loading Logic (Append only new records)
            if load_method == 'append' and date_col:
                logger.info(f"Executing incremental load for {table_name}")
                # Check if table exists to get the max date
                table_exists_query = f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = '{TARGET_SCHEMA}' AND table_name = '{table_name}'
                    );
                """
                table_exists = connection.execute(text(table_exists_query)).scalar()

                if table_exists:
                    max_date_query = f"SELECT MAX({date_col}) FROM {TARGET_SCHEMA}.{table_name}"
                    max_date = connection.execute(text(max_date_query)).scalar()

                    if max_date:
                        logger.info(f"Filtering records after {max_date}")
                        # Filter DataFrame for new records only
                        df = df[df[date_col] > pd.to_datetime(max_date)]

                if df.empty:
                    logger.info(f"No new records to append for {table_name}. Skipping load.")
                    return

            # Execute the Load
            logger.info(f"Loading {len(df)} rows into {TARGET_SCHEMA}.{table_name}")
            df.to_sql(
                name=table_name,
                con=connection,
                schema=TARGET_SCHEMA,
                if_exists=load_method, # 'replace' or 'append'
                index=False,
                method='multi', # Inserts multiple rows in a single INSERT statement
                chunksize=5000
            )

        logger.info(f"Successfully loaded {table_name}")

    except pd.errors.EmptyDataError:
        logger.error(f"The source file {file_path} is empty.")
        raise
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred while loading {table_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during ingestion of {table_name}: {str(e)}")
        raise

# ==========================================
# 3. DAG Definition
# ==========================================
with DAG(
    'telco_daily_ingestion',
    default_args=default_args,
    description='Extracts synthetic Telco data and loads it into PostgreSQL staging',
    schedule='@daily', # Changed from schedule_interval to schedule
    catchup=False,
    tags=['telco', 'ingestion', 'staging']
) as dag:

    # Full Load Tasks (Dimension staging tables)
    # We replace these entirely as they are small and manage state.
    # dbt snapshots will handle the historical tracking later.
    load_plans = PythonOperator(
        task_id='load_stg_plans',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_service_plans', 'file_name': 'service_plans.csv', 'load_method': 'replace'}
    )

    load_customers = PythonOperator(
        task_id='load_stg_customers',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_customers', 'file_name': 'customers.csv', 'load_method': 'replace'}
    )

    load_subscriptions = PythonOperator(
        task_id='load_stg_subscriptions',
        python_callable=ingest_data,
        op_kwargs={'table_name': 'stg_subscriptions', 'file_name': 'subscriptions.csv', 'load_method': 'replace'}
    )

    # Incremental Load Tasks (Fact staging tables)
    # We append to these to simulate processing high-volume transactional data.
    load_usage = PythonOperator(
        task_id='load_stg_network_usage',
        python_callable=ingest_data,
        op_kwargs={
            'table_name': 'stg_network_usage',
            'file_name': 'network_usage.csv',
            'load_method': 'append',
            'date_col': 'usage_date'
        }
    )

    load_billing = PythonOperator(
        task_id='load_stg_billing',
        python_callable=ingest_data,
        op_kwargs={
            'table_name': 'stg_billing_invoices',
            'file_name': 'billing_invoices.csv',
            'load_method': 'append',
            'date_col': 'billing_period_start'
        }
    )

    # Define Dependencies
    [load_plans, load_customers] >> load_subscriptions
    load_subscriptions >> [load_usage, load_billing]