# Growth Analytics System — Two-Pipeline Data Platform


<img width="3189" height="2027" alt="Flowchart (1)" src="https://github.com/user-attachments/assets/c4bf8f86-7fde-46af-98f8-b9302779c666" />


**Tech stack:** Spark, Apache Airflow, Apache Iceberg, Scala, SQL, Parquet, S3, Docker

---

## Table of contents
1. [Project Overview](#project-overview)  
2. [Goals & Requirements](#goals--requirements)  
3. [High-level architecture](#high-level-architecture)  
4. [Pipelines](#pipelines)  
   - [Pipeline 1 — Creating the Fact table with WAP](#pipeline-1--creating-the-fact-table-with-wap)  
   - [Pipeline 2 — Aggregation & Analytics Table](#pipeline-2--aggregation--analytics-table)  
5. [Storage & Table Layout (Apache Iceberg)](#storage--table-layout-apache-iceberg)  
6. [Performance & Optimization Strategies](#performance--optimization-strategies)  
7. [SQL / Semantic decisions](#sql--semantic-decisions)  
<!--- 8. [Implementation details & code snippets](#implementation-details--code-snippets)  
   - [Airflow DAG skeleton](#airflow-dag-skeleton)  
   - [Spark submit example](#spark-submit-example)  
   - [Example Scala aggregation snippet (pseudocode)](#example-scala-aggregation-snippet-pseudocode)  
   - [GROUPING SETS example (SQL)](#grouping-sets-example-sql)  
   - [Iceberg table creation (example)](#iceberg-table-creation-example)  
9. [Local development & testing](#local-development--testing)  
10. [CI / Deployment](#ci--deployment)  
11. [Monitoring, logging & validation](#monitoring-logging--validation)  
12. [Repository layout (recommended)](#repository-layout-recommended)  
13. [Future improvements](#future-improvements)  
14. [Contributing](#contributing)  
15. [Contact / Authors / License](#contact--authors--license) --->



## Project overview
This repository contains code, orchestration, and documentation for a **Growth Analytics System** built as a two-pipeline data platform:

- **Pipeline 1:** Daily batch ingestion that extracts data from OLTP and CSV sources, cleans andtransforms it, and loads a staging fact table for Write-Audit-Publish pattern before pushing it into production tabe.
- **Pipeline 2:** Downstream aggregation pipeline that computes analytic aggregates into an aggregated analytics table. Both pipelines must run in strict sequence (ingestion → aggregation).

Key design points:
- Orchestration with **Apache Airflow** (ensures sequencing, retries).
- Storage with **Apache Iceberg** for facts and dimension tables (ACID, schema evolution).
- Spark jobs for ETL; aggregation pipeline implemented in **Scala** to avoid Python UDF overhead at scale.
- Partitioning, bucketing, and sort-order optimizations to maximize compression (RLE) and minimize shuffle.



## Goals & Requirements
- Reliable, repeatable daily ingestion of OLTP & CSV sources.
- Deterministic order of execution: staging load completes before aggregation runs.
- Efficient storage and read/write patterns at scale (Iceberg).
- Minimize Spark shuffle and serialization overhead.



## High-level architecture
1. **Data sources**: OLTP DB (e.g., PostgreSQL / MySQL) + CSV drops (SFTP / S3).
2. **Ingestion (Pipeline 1)**: Spark jobs read sources → cleaning/transformations → write staging fact table in Iceberg.
3. **Orchestration**: Airflow DAG ensures ingestion completes, then triggers aggregation.
4. **Aggregation (Pipeline 2)**: Spark (Scala) jobs read staging Iceberg tables → perform aggregations → write aggregate analytics table (Iceberg).
5. **Downstream**: BI tools or analytics jobs read the aggregated table.




## Pipelines

### Pipeline 1 — Creating the Fact table with WAP
- **Frequency:** Daily batch.
- **Inputs:** OLTP (via JDBC), CSV files (S3).
- **Transformations:** cleansing, type normalization, basic deduplication.
- **Important:** Partitioning and pre-sort applied at write time (see performance section).

### Pipeline 2 — Aggregation & Analytics Table
- **Runs:** Only after Pipeline 1 completes successfully (Airflow sequencing).
- **Implementation:** Scala-based Spark job implementing aggregations and grouping logic (GROUPING SETS, rollups where necessary).
- **Why Scala:** Avoid Python UDFs and Python-JVM serialization/deserialization overhead. Scala functions run natively on the JVM and provide better performance at big-data scale.
- **Join strategies experimented:** Sort-Merge-Join (SMJ), Bucketed Join, Broadcast Join — chosen based on data size and skew to minimize shuffles.


## Storage & table layout (Apache Iceberg)
- **Table format:** Apache Iceberg (enables ACID, metadata, time travel, partition evolutions).
- **Data file format:** Parquet.
- **Partitioning:** Implement partition pruning-friendly columns; align partition keys with common query predicates (e.g., `event_date`, `country`).
- **Bucketing:** Used for heavy join keys to enable efficient bucketed joins.
- **On-disk sorting:** Columns sorted from **low → high cardinality** to maximize RLE compression (low-cardinality columns first for longer repeated runs).
- **Compaction & rewriting:** Periodic file compaction and rewrite strategies to keep file sizes optimal for Spark throughput.



## Performance & optimization strategies
- **RLE maximization**: Sort columns from low-to-high cardinality before write so RLE in Parquet compresses better — reduces storage and reduces IO cloud bill.
- **Join strategy selection:** Benchmarked Sort-Merge Join, Bucketed Joins, and Broadcast Joins. Use broadcast when one side is significantly smaller; use bucketed joins when both sides are large but can be pre-bucketed on join keys to avoid shuffle.
- **Partition pruning:** Use partitioning that aligns with common filters. Design queries to include partition columns in WHERE clauses.
- **Bucketing:** Physically bucket large tables by join key to enable Spark to perform bucketed joins with no wide shuffle.
- **Scala for aggregation:** Avoid Python UDFs (they cause serialization and deserialization overhead) — write aggregation logic in Scala to achieve native performance on the JVM.
- **Avoid unnecessary MERGE where INSERT OVERWRITE is sufficient:** `MERGE` has stronger semantics but more overhead; prefer `INSERT OVERWRITE` for full-partition refreshes where appropriate.



## SQL / semantic decisions
- **GROUPING SETS:** Used for multi-level aggregation in a single query and to reduce multiple passes over data.
- **INSERT OVERWRITE vs MERGE INTO:**
  - Use `INSERT OVERWRITE` for full-partition refresh patterns — simpler & faster.
  - Use `MERGE INTO` for incremental upserts where you must update existing rows based on keys.

<!---## Implementation details & code snippets

### Airflow DAG skeleton
```python
# dags/growth_analytics_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='growth_analytics_two_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def run_ingest(**kwargs):
        # call spark-submit for ingestion job
        pass

    def run_aggregation(**kwargs):
        # call spark-submit for aggregation job
        pass

    ingest_task = PythonOperator(
        task_id='ingest_to_staging',
        python_callable=run_ingest,
    )

    aggregation_task = PythonOperator(
        task_id='run_aggregation',
        python_callable=run_aggregation,
    )

    ingest_task >> aggregation_task
--- >
