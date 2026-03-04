Senior Data Engineer Challenge: Telco Customer Analytics
Overview
This repository contains the end-to-end implementation of a modernized data warehouse designed for Veracity Digital's telecommunications partner. The project encompasses synthetic data generation, an Airflow-orchestrated ELT pipeline, advanced dimensional modeling with dbt, and business intelligence querying.

a. Setup and Execution Instructions
Prerequisites
Docker & Docker Compose

Python 3.10+ (with virtualenv)

Git ### Step 1: Infrastructure Initialization
Start the local target PostgreSQL data warehouse and pgAdmin UI using Docker. This automatically creates the necessary telco_db database and the raw_staging schema via the init.sql script.

Bash
docker-compose up -d
Access pgAdmin at http://localhost:5050 (User: admin@telco.com, Pass: admin_password).

Step 2: Data Generation (Source Simulation)
Generate the 6-month synthetic telco dataset (10,000 customers).

Bash
python3 -m venv myprojectenv
source myprojectenv/bin/activate
pip install pandas numpy faker
python generate_telco_data.py
This outputs 5 CSV files into the /data directory representing the operational source system.

Step 3: Pipeline Orchestration (Airflow)
Ensure your Airflow environment is configured with the Postgres connection (telco_postgres_dw).

Place telco_etl_dag.py in your Airflow dags/ folder.

Unpause the telco_master_elt_pipeline DAG in the Airflow UI.

Trigger the DAG. It will automatically:

Extract data from the CSVs.

Load it into the raw_staging PostgreSQL schema.

Run a Data Quality circuit breaker.

Execute the downstream dbt transformations.

Step 4: DBT Transformations (Manual Run Optional)
To manually build the dimensional model and run the constraint tests:

Bash
cd telco_dwh_dbt
dbt deps
dbt build --profiles-dir .
b. Data Modeling & Architecture
Source System (OLTP) ER Diagram
The simulated operational system is designed in 3rd Normal Form (3NF) to ensure transactional integrity. It separates the core catalog (service_plan) from the highly mutable lifecycle tracking (subscription) and demographic data (customer). High-velocity, append-only telemetry is stored in network_usage, while financial records sit in billing_invoice.

Data Warehouse (OLAP) Dimensional Model
The target warehouse utilizes a Kimball Star Schema optimized for analytical workloads.

fact_service_usage (Daily Grain): Captures atomic network telemetry (data consumed, dropped calls).

fact_billing_events (Monthly Grain): Captures financial snapshot data per invoice.

dim_customer (SCD Types 1, 2, & 3): The cornerstone of the model. It tracks demographics and handles subscription transitions dynamically.

SCD Type 1: Overwrites the customer_segment to always reflect current status.

SCD Type 2: Uses valid_from and valid_to to track historical service_plan_type transitions.

SCD Type 3: Maintains the previous_service_plan to quickly analyze downgrade/upgrade paths without complex self-joins.

c. Justification of Theoretical Choices
Data Warehousing Methodologies: Kimball vs. Inmon
For this analytics platform, a Kimball-centric dimensional modeling strategy was chosen. While Inmon’s top-down, 3NF Enterprise Data Warehouse approach offers a single source of truth, it is highly complex and slower to deliver business value. The Kimball approach (Star Schemas) natively supports the aggregations required by BI tools to answer questions about churn and revenue. Furthermore, Kimball’s strict frameworks for Slowly Changing Dimensions are vital for modeling the Telco customer lifecycle accurately over time.

Modern Data Stack Philosophy: Airflow & dbt vs. Traditional ETL
Traditional ETL tools (e.g., SSIS) are monolithic, tightly coupling orchestration with GUI-driven transformations. This makes version control, testing, and scaling incredibly difficult.
We adopted the Modern Data Stack (ELT) paradigm:

Separation of Concerns: Apache Airflow handles purely the "when" and "how" (orchestration, retries, dependency management), while dbt handles the "what" (SQL-based transformations pushed down to the warehouse compute).

Software Engineering Best Practices: dbt allows data transformations to be treated as code. This enables Git version control, peer reviews, CI/CD pipelines, and automated, built-in data quality testing—features that are either missing or highly convoluted in traditional ETL tools.

d. Change Data Capture (CDC) Strategy
To keep the data warehouse synchronized with the operational systems without degrading source performance, a dual-strategy CDC approach is recommended for production:

For Mutable Data (customers, subscriptions): A Log-Based CDC approach (e.g., Debezium streaming from PostgreSQL WAL to Kafka) is required. Relying on timestamps misses intermediate changes and hard deletes. Log-based CDC captures every exact state change (Insert, Update, Delete) in real-time, allowing downstream dbt models to flawlessly construct exact SCD Type 2 timelines.

For Immutable Data (network_usage): A Timestamp-Based (High-Water Mark) approach is sufficient and highly efficient. Since telemetry data is append-only, Airflow can simply query the operational table for usage_date > max_processed_date, avoiding the infrastructure overhead of log-based streaming for high-volume, low-complexity events.

e. Time Spent on Assignment
Phase 1: Architecture & Dimensional Design: 2 hours

Phase 2: Data Generation Simulation (Python/Pandas): 2.5 hours

Phase 3: ETL Pipeline (Airflow Orchestration): 2.5 hours

Phase 4: Transformation & SCD Modeling (dbt): 3 hours

Phase 5: Analytical Queries & Documentation: 2 hours

Total Estimated Time: ~12 hours
