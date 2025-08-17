import os
import sys
from datetime import date, datetime
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    row_number,
    to_date,
    current_date,
    expr,
    broadcast,
    trunc,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline1")


def get_spark(app_name: str = "pipeline1_fct_build") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_jdbc_table(spark: SparkSession, jdbc_url: str, table: str, properties: dict):
    logger.info(f"Reading JDBC table {table}")
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", properties.get("user"))
        .option("password", properties.get("password"))
        .option("driver", properties.get("driver", "org.postgresql.Driver"))
        .load()
    )


def append_to_iceberg(df, table_name: str):
    logger.info(f"Appending to Iceberg table {table_name}")
    try:
        df.writeTo(table_name).append()
    except Exception as e:
        df.write.mode("append").saveAsTable(table_name)


def overwrite_partitions_iceberg(df, table_name: str):
    try:
        df.writeTo(table_name).overwritePartitions()
    except Exception as e:
        df.write.mode("overwrite").insertInto(table_name)


def ingest_raw(spark, jdbc_url, props):
    raw_tables = {
        "billing_events": "saas.raw_billing_events",
        "subscriptions": "saas.raw_subscriptions",
        "customers": "saas.raw_customers",
    }

    for src_table, dest_table in raw_tables.items():
        df = read_jdbc_table(spark, jdbc_url, src_table, props)
        df = df.withColumn("_ingest_ts", lit(datetime.utcnow().isoformat()))
        append_to_iceberg(df, dest_table)
        logger.info(f"Ingested {src_table} -> {dest_table} rows={df.count()}")


def stage_subscriptions(spark):
    raw = spark.table("saas.raw_subscriptions")

    # Filter required keys not null
    stg = (
        raw.filter(
            (col("subscription_id").isNotNull())
            & (col("customer_id").isNotNull())
            & (col("plan_id").isNotNull())
        )
        # ds for audit and staging
        .withColumn("ds", current_date())
    )

    # Insert overwrite semantics
    overwrite_partitions_iceberg(stg, "saas.stg_subscriptions")
    logger.info(f"Staged subscriptions rows={stg.count()}")


def dedup_billing_events(spark):
    raw = spark.table("saas.raw_billing_events")

    payments = raw.filter(col("event_type") == "payment_succeeded")

    w = Window.partitionBy("event_id").orderBy(col("event_date").desc())
    deduped = (
        payments.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("ds", current_date())
    )

    overwrite_partitions_iceberg(deduped, "saas.stg_billing_events")
    logger.info(f"Deduped billing events rows={deduped.count()}")


def scd2_customers_merge(spark):
    today = date.today()
    ds_today = current_date()

    incoming = (
        spark.table("saas.raw_customers")
        .select(
            col("customer_id"),
            col("name"),
            col("status"),
            col("country"),
            col("signup_date"),
        )
        .filter(col("customer_id").isNotNull())
        .withColumn("start_date", ds_today)
        .withColumn("end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    # using merge into 
    try:
        incoming.createOrReplaceTempView("_incoming_customers")

        merge_sql = f"""
MERGE INTO saas.dim_customers_scd2 t
USING _incoming_customers s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (
  t.name <> s.name OR
  t.status <> s.status OR
  t.country <> s.country OR
  t.signup_date <> s.signup_date
) THEN
  UPDATE SET t.is_current = false, t.end_date = current_date()
WHEN NOT MATCHED THEN
  INSERT (customer_id, name, status, country, signup_date, start_date, end_date, is_current, ds)
  VALUES (s.customer_id, s.name, s.status, s.country, s.signup_date, current_date(), NULL, true, current_date())
"""
        spark.sql(merge_sql)

    except Exception as e:
        existing = spark.table("saas.dim_customers_scd2")

        # find changed customers
        joined = existing.alias("t").join(
            incoming.alias("s"), on=[existing.customer_id == incoming.customer_id], how="right")

        # expired rows: existing current rows that differ
        expired = (
            existing.alias("t")
            .join(incoming.alias("s"), on=[existing.customer_id == incoming.customer_id])
            .filter((existing.is_current == True) & (
                (existing.name != incoming.name)
                | (existing.status != incoming.status)
                | (existing.country != incoming.country)
                | (existing.signup_date != incoming.signup_date)
            ))
            .withColumn("is_current", lit(False))
            .withColumn("end_date", ds_today)
            .select(*existing.columns)
        )

        # new versions to insert
        new_versions = (
            incoming.select(
                "customer_id",
                "name",
                "status",
                "country",
                "signup_date",
                "start_date",
                "end_date",
                "is_current",
                "ds",
            )
        )
        keep = (
            existing.join(expired.select("customer_id"), on="customer_id", how="left_anti")
        )

        final = keep.unionByName(expired).unionByName(new_versions)

        overwrite_partitions_iceberg(final, "saas.dim_customers_scd2")

    logger.info("SCD2 customers merge complete")



def quality_checks(df_fact, inputs_map: dict):
    messages = []
    ok = True

    # Non-null checks for business keys
    null_key_count = df_fact.filter(
        col("event_id").isNull() | col("subscription_id").isNull() | col("customer_id").isNull()
    ).count()
    messages.append(f"Non-null key violations: {null_key_count}")
    if null_key_count > 0:
        ok = False

    # Duplicate check on event_id
    total = df_fact.count()
    distinct_event_ids = df_fact.select("event_id").distinct().count()
    dup_count = total - distinct_event_ids
    messages.append(f"Duplicate event_id count: {dup_count}")
    if dup_count > 0:
        ok = False

    # Revenue sanity
    neg_amounts = df_fact.filter(col("m_amount") < 0).count()
    messages.append(f"Negative amount rows: {neg_amounts}")
    if neg_amounts > 0:
        ok = False

    # Compare counts
    src_events = inputs_map.get("stg_billing_events", None)
    if src_events is not None:
        messages.append(f"Fact rows: {total}, Source stg_billing_events: {src_events}")
        # Basic threshold: fact should be <= source and > 0
        if total == 0 or total > src_events:
            ok = False

    return ok, messages

def build_fact_table(spark):
    # Load staging tables
    stg_billing = spark.table("saas.stg_billing_events")
    stg_subs = spark.table("saas.stg_subscriptions")
    dim_customers = (
        spark.table("saas.dim_customers_scd2").filter(col("is_current") == True)
    )

    # Repartition to emulate bucketed join on subscription_id
    buckets = 16
    stg_billing_repart = stg_billing.repartition(buckets, "subscription_id")
    stg_subs_repart = stg_subs.repartition(buckets, "subscription_id")

    # Join billing <> subscriptions (bucketed join - co-partitioned by subscription_id)
    joined = (
        stg_billing_repart.alias("b")
        .join(stg_subs_repart.alias("s"), on=["subscription_id"], how="inner")
    )

    # Broadcast join to customers (small)
    joined_with_cust = (
        joined.join(broadcast(dim_customers.alias("c")), on=["customer_id"], how="left")
    )

    # Build final fact columns
    fact = (
        joined_with_cust.select(
            col("customer_id"),
            col("c.name").alias("dim_name"),
            col("c.status").alias("dim_customer_status"),
            col("c.signup_date").alias("dim_signup_date"),
            col("s.end_date").alias("dim_subscription_end_date"),
            col("subscription_id"),
            col("event_id"),
            col("event_type").alias("dim_event_type"),
            col("amount").alias("m_amount"),
            col("plan_id"),
            col("event_date").alias("dim_event_date"),
            col("c.country").alias("dim_country"),
            trunc(col("event_date"), "MM").alias("month_start"),
            current_date().alias("ds"),
        )
    )

    # Sort within partitions by low-cardinality columns for RLE friendliness
    sort_cols = ["dim_event_date", "dim_country", "dim_customer_status"]
    fact_sorted = fact.sortWithinPartitions(*sort_cols)

    # Write to staging location for WAP: write to a staging table / partition (e.g., ds=current_date())
    staging_table = "saas.fct_billing_events_staging"
    overwrite_partitions_iceberg(fact_sorted, staging_table)

    logger.info(f"Built staging fact rows={fact_sorted.count()}")
    return fact_sorted


def wap_and_publish(spark, fact_df):
    staging_table = "saas.fct_billing_events_staging"
    prod_table = "saas.fct_billing_events"

    # Gather input counts for QC comparators
    stg_count = spark.table("saas.stg_billing_events").count()
    inputs_map = {"stg_billing_events": stg_count}

    qc_ok, qc_messages = quality_checks(fact_df, inputs_map)

    for m in qc_messages:
        logger.info("QC: %s" % m)

    if not qc_ok:
        logger.error("QC failed. Aborting publish. Leaving staging intact.")
        raise RuntimeError("Quality checks failed: " + "; ".join(qc_messages))
    
    staging = spark.table(staging_table)
    overwrite_partitions_iceberg(staging, prod_table)


def run_pipeline(jdbc_url: str, jdbc_props: dict):
    spark = get_spark()
    ingest_raw(spark, jdbc_url, jdbc_props)
    stage_subscriptions(spark)
    dedup_billing_events(spark)
    scd2_customers_merge(spark)
    fact_df = build_fact_table(spark)
    wap_and_publish(spark, fact_df)