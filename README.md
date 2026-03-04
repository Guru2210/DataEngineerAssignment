# Senior Data Engineer Challenge: Telco Customer Analytics

> **Veracity Digital** — End-to-end modernized data warehouse for a telecommunications partner.

---

## 📋 Overview

This repository contains a complete implementation of a modern ELT data platform built for telco customer analytics. The solution covers:

- 🏭 **Synthetic Data Generation** — 10,000 customers, 6 months of telco data
- ⚙️ **Airflow Orchestration** — Automated ELT pipeline with data quality checks
- 🧱 **Dimensional Modeling (dbt)** — Kimball Star Schema with SCD Types 1, 2 & 3
- 📊 **Business Intelligence Queries** — Analytical SQL for churn & revenue insights

---

## 🗂️ Repository Structure

```
DataEngineerAssignment/
├── generate_telco_data.py       # Synthetic data generator (source simulation)
├── telco_etl_dag.py             # Airflow ELT DAG
├── telco_etl_dag_advanced.py    # Advanced Airflow DAG variant
├── requirements.txt             # Python dependencies
├── telco_dwh_dbt/               # dbt project (transformations & tests)
│   ├── models/                  # Staging, intermediate & mart models
│   ├── macros/                  # Reusable Jinja macros
│   ├── tests/                   # Custom data quality tests
│   ├── packages.yml             # dbt package dependencies
│   └── dbt_project.yml          # dbt project configuration
└── README.md
```

---

## ⚙️ Setup & Execution Instructions

### Prerequisites

Ensure the following are installed before proceeding:

| Tool | Version |
|---|---|
| Docker & Docker Compose | Latest |
| Python | 3.10+ |
| Git | Latest |

---

### Step 1 — Infrastructure Initialization

Start the local PostgreSQL data warehouse and pgAdmin UI using Docker. The `init.sql` script automatically creates the `telco_db` database and `raw_staging` schema.

```bash
docker-compose up -d
```

> 🌐 Access pgAdmin at **http://localhost:5050**
> - **User:** `admin@telco.com`
> - **Password:** `admin_password`

---

### Step 2 — Data Generation (Source Simulation)

Generate the 6-month synthetic telco dataset for 10,000 customers.

```bash
python3 -m venv myprojectenv
source myprojectenv/bin/activate        # On Windows: myprojectenv\Scripts\activate
pip install -r requirements.txt
python generate_telco_data.py
```

This outputs **5 CSV files** into the `/data` directory, simulating an operational source system:

| File | Description |
|---|---|
| `customers.csv` | Customer demographics |
| `service_plans.csv` | Service plan catalog |
| `subscriptions.csv` | Customer subscription lifecycle |
| `network_usage.csv` | Daily telemetry (data, dropped calls) |
| `billing_invoices.csv` | Monthly financial records |

---

### Step 3 — Pipeline Orchestration (Airflow)

1. Configure your Airflow environment with a PostgreSQL connection named **`telco_postgres_dw`**.
2. Place `telco_etl_dag.py` in your Airflow `dags/` folder.
3. In the Airflow UI, **unpause** the `telco_master_elt_pipeline` DAG.
4. **Trigger** the DAG — it will automatically:
   - Extract data from the CSV source files
   - Load raw data into the `raw_staging` PostgreSQL schema
   - Run a **Data Quality circuit breaker**
   - Execute downstream **dbt transformations**

---

### Step 4 — dbt Transformations (Manual Run)

To manually build the dimensional model and execute constraint tests:

```bash
cd telco_dwh_dbt
dbt deps
dbt build --profiles-dir .
```

---

## 🏛️ Data Modeling & Architecture

### Source System (OLTP) — Entity Relationship Design

The simulated operational system is designed in **3rd Normal Form (3NF)** for transactional integrity:

| Table | Role |
|---|---|
| `service_plan` | Core catalog (immutable) |
| `customer` | Demographic data |
| `subscription` | Mutable lifecycle tracking |
| `network_usage` | High-velocity, append-only telemetry |
| `billing_invoice` | Financial records |

---

### Data Warehouse (OLAP) — Kimball Star Schema

The target warehouse uses a **Star Schema** optimized for analytical workloads:

#### Fact Tables

| Table | Grain | Description |
|---|---|---|
| `fact_service_usage` | Daily | Captures atomic network telemetry (data consumed, dropped calls) |
| `fact_billing_events` | Monthly | Captures financial snapshot data per invoice |

#### Dimension Tables

| Table | SCD Strategy | Description |
|---|---|---|
| `dim_customer` | Types 1, 2 & 3 | Cornerstone dimension tracking customer demographics and subscription transitions |

**SCD Type Breakdown for `dim_customer`:**

- **SCD Type 1** — Overwrites `customer_segment` to always reflect the current status.
- **SCD Type 2** — Uses `valid_from` / `valid_to` columns to track historical `service_plan_type` transitions.
- **SCD Type 3** — Maintains a `previous_service_plan` column for quick upgrade/downgrade analysis without complex self-joins.

---

## 🧠 Justification of Theoretical Choices

### Data Warehousing: Kimball vs. Inmon

A **Kimball-centric** dimensional modeling strategy was chosen for this platform.

| Factor | Kimball (Chosen) | Inmon |
|---|---|---|
| Approach | Bottom-up, Star Schema | Top-down, 3NF EDW |
| Speed to Value | Fast | Slow |
| BI Tool Support | Native aggregation support | Requires transformation layer |
| SCD Handling | First-class, built-in patterns | Complex to implement |
| Complexity | Lower | Higher |

Kimball's Star Schemas natively support the aggregations needed to answer churn and revenue questions, while its SCD frameworks are essential for modeling the telco customer lifecycle accurately over time.

---

### Modern Data Stack: Airflow + dbt vs. Traditional ETL

Traditional ETL tools (e.g., SSIS) are **monolithic** — they tightly couple orchestration with GUI-driven transformations, making version control, testing, and scaling difficult.

This project adopts the **Modern Data Stack (ELT)** paradigm:

| Concern | Tool | Responsibility |
|---|---|---|
| **Orchestration** | Apache Airflow | *When* and *how* (scheduling, retries, dependency management) |
| **Transformation** | dbt | *What* (SQL-based transformations pushed to warehouse compute) |

**Key advantages of dbt over traditional ETL:**
- ✅ Git version control for all transformations
- ✅ Peer-reviewable SQL models
- ✅ CI/CD pipeline integration
- ✅ Built-in automated data quality testing

---

## 🔄 Change Data Capture (CDC) Strategy

To keep the warehouse synchronized with operational systems without degrading source performance, a **dual-strategy CDC approach** is recommended for production:

### Mutable Data (Customers, Subscriptions)
**→ Log-Based CDC** (e.g., Debezium → PostgreSQL WAL → Kafka)

Timestamp-based approaches miss intermediate changes and hard deletes. Log-based CDC captures every state change (INSERT, UPDATE, DELETE) in real-time, enabling dbt to construct exact SCD Type 2 timelines.

### Immutable Data (Network Usage)
**→ Timestamp-Based / High-Water Mark**

Since telemetry data is **append-only**, Airflow can efficiently query:
```sql
WHERE usage_date > max_processed_date
```
This avoids the infrastructure overhead of log-based streaming for high-volume, low-complexity events.

---

## ⏱️ Time Spent on Assignment

| Phase | Task | Time |
|---|---|---|
| Phase 1 | Architecture & Dimensional Design | 2.0 hrs |
| Phase 2 | Data Generation Simulation (Python/Pandas) | 2.5 hrs |
| Phase 3 | ETL Pipeline (Airflow Orchestration) | 2.5 hrs |
| Phase 4 | Transformation & SCD Modeling (dbt) | 3.0 hrs |
| Phase 5 | Analytical Queries & Documentation | 2.0 hrs |
| | **Total** | **~12 hours** |

---

## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?style=flat&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Core-FF694B?style=flat&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-4169E1?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)
