package com.yourorg.saas
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{current_date, col}
import org.slf4j.LoggerFactory

object AggregationJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("pipeline2_agg_billing_analytics")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")
      logger.info("Starting Pipeline 2 — Aggregation & Analytics")

      val fact = spark.table("saas.fct_billing_events")
      val plans = spark.table("saas.plans")

      fact.createOrReplaceTempView("fct_billing_events")
      plans.createOrReplaceTempView("plans")

      val aggSql =
        """
        SELECT
          CASE
            WHEN GROUPING(p.name) = 1 AND GROUPING(dim_country) = 1 THEN 'overall'
            WHEN GROUPING(p.name) = 0 AND GROUPING(dim_country) = 0 THEN 'plan_country'
            WHEN GROUPING(p.name) = 0 THEN 'plan'
            WHEN GROUPING(dim_country) = 0 THEN 'country'
          END AS aggregation_level,
          COALESCE(p.name, 'overall') AS dim_plan,
          COALESCE(dim_country, 'overall') AS dim_country,
          month_start,
          COUNT(1) AS m_total_events,
          SUM(m_amount) AS m_revenue
        FROM fct_billing_events f
        JOIN plans p
          ON f.plan_id = p.plan_id
        WHERE dim_event_type = 'payment_succeeded'
        GROUP BY GROUPING SETS (
          (p.name, month_start),
          (dim_country, month_start),
          (p.name, dim_country, month_start),
          (month_start),
          ()
        )
        ORDER BY aggregation_level
        """

      val aggDfRaw: DataFrame = spark.sql(aggSql)

      // adding ds for lineage
      val aggDf = aggDfRaw.withColumn("execution_date", current_date())

      val rowCount = aggDf.count()
      logger.info(s"Aggregation produced $rowCount rows")

      aggDf.writeTo("saas.agg_billing_analytics").overwritePartitions()

      val writtenCount = spark.table("saas.agg_billing_analytics").count()

    } catch {
      case t: Throwable =>
        logger.error("job failed", t)
        throw t
    } finally {
      spark.stop()
    }
  }
}
