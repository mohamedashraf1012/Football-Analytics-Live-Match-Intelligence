# FootballFlow — Airflow Orchestration

Apache Airflow 2.9.2 running on Docker with LocalExecutor and PostgreSQL.

## Pipeline

```
check_bronze_files       S3KeySensor — waits for all 12 CSVs in bronze/
    >> run_silver_cleaning     runs run_all_silver.py (PySpark, single session)
    >> check_silver_files      confirms Parquet output exists in silver/
    >> load_to_snowflake       COPY INTO all 12 RAW tables
    >> dbt_run_staging         RAW → STAGING
    >> dbt_snapshot_players    SCD Type 2 for player history
    >> dbt_run_dims_facts      Star Schema dims + facts
    >> dbt_run_marts           aggregated mart tables for Power BI
    >> dbt_test                data quality tests
```

**Schedule:** Daily at 02:00 UTC (04:00 Cairo)

---

## Folder structure

```
footballflow-airflow/
├── docker-compose.yml
├── Dockerfile                  custom Airflow image with Java 11 + PySpark
├── requirements.txt
├── .env.example                copy to .env and fill in credentials
│
├── dags/
│   ├── footballflow_batch_dag.py
│   └── utils/
│       ├── s3_helpers.py           Bronze/Silver S3 checks
│       ├── snowflake_copy_sql.py   COPY INTO statements for all 12 tables
│       └── notifications.py        failure/success email callbacks
│
├── scripts/
│   └── setup_connections.sh    creates Airflow connections from .env
│
├── dbt/
│   └── profiles.yml
│
├── project/                    mount your Silver layer code here
├── logs/
└── plugins/
```

---

## Setup

### 1. Configure credentials

```bash
cp .env.example .env
# Fill in AWS, Snowflake, and Airflow secret key values
```

### 2. Copy Silver project code

```bash
cp -r /path/to/your/silver/project/* ./project/
```

The `project/` folder needs `run_all_silver.py`, `spark_session.py`, `config.py`, and the `cleaning/` package.

### 3. Set up Snowflake (one-time)

Copy the `SETUP_SQL` block from `dags/utils/snowflake_copy_sql.py` and run it in your Snowflake worksheet. This creates the database, schemas, Parquet file format, and S3 external stage.

### 4. Build and start Airflow

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

UI at **http://localhost:8080** — username `admin`, password `admin`.

### 5. Create Airflow connections

```bash
chmod +x scripts/setup_connections.sh
./scripts/setup_connections.sh
```

Or create manually at Admin → Connections in the UI.

**snowflake_footballflow**

| Field | Value |
|---|---|
| Connection Type | Snowflake |
| Account | your account identifier (e.g. `abc123.eu-west-1`) |
| Login | your Snowflake username |
| Password | your Snowflake password |
| Schema | `RAW` |
| Warehouse | `COMPUTE_WH` |
| Database | `FOOTBALLFLOW` |
| Role | `ACCOUNTADMIN` |

### 6. Trigger the DAG

Toggle `footballflow_batch_pipeline` on and hit ▶ Trigger DAG.

---

## Useful commands

```bash
# Live scheduler logs
docker compose logs -f airflow-scheduler

# Shell into scheduler container
docker exec -it footballflow_airflow_scheduler bash

# Re-run a single task
docker exec -it footballflow_airflow_scheduler \
    airflow tasks run footballflow_batch_pipeline <task_id> <execution_date>

# Parse DAG without executing
docker exec -it footballflow_airflow_scheduler \
    airflow dags test footballflow_batch_pipeline 2025-01-01

# Full reset
docker compose down --volumes --remove-orphans
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| DAG not showing | Check scheduler logs for import errors |
| `ModuleNotFoundError: utils` | Confirm `dags/utils/__init__.py` exists |
| Spark fails — `JAVA_HOME not set` | `docker compose build --no-cache` |
| S3 access denied | Check AWS keys in `.env` |
| Snowflake connection fails | Account format should be `abc123.eu-west-1` — no `.snowflakecomputing.com` |
| Docker out of memory | Allocate 6 GB+ RAM in Docker Desktop Settings |
| Port 8080 in use | Change to `"8081:8080"` in `docker-compose.yml` |
| dbt profiles not found | Check `DBT_PROFILES_DIR` in `.env` points to the folder containing `profiles.yml` |
