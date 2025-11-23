package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import platform.{Fetchers, SparkPlatformTrait}

/** Backfill pipeline: transforms events_raw → features_daily.
  *
  * Computes daily feature aggregations from raw events:
  * - 7-day and 30-day event counts
  * - Days since last event
  * - Event type counts
  *
  * Common use patterns:
  * {{{
  *   val platform = PlatformProvider.createLocal("backfill-job")
  *   val pipeline = new BackfillPipeline(platform)
  *   pipeline.run(
  *     eventsRawPath = "s3://bucket/events_raw",
  *     outputTable = "feature_store.features_daily",
  *     startDate = "2024-01-01",
  *     endDate = "2024-12-31"
  *   )
  *   platform.stop()
  * }}}
  */
class BackfillPipeline(platform: SparkPlatformTrait) {

  private val spark: SparkSession = platform.spark

  /** Runs the backfill pipeline.
    *
    * @param eventsRawPath Path to events_raw table/data
    * @param outputTable Output Iceberg table name (e.g., "db.features_daily")
    * @param startDate Start date for backfill (YYYY-MM-DD)
    * @param endDate End date for backfill (YYYY-MM-DD)
    */
  def run(
      eventsRawPath: String,
      outputTable: String,
      startDate: String,
      endDate: String
  ): Unit = {
    import spark.implicits._

    // Read events_raw
    val eventsRaw = Fetchers.readParquet(spark, eventsRawPath, Some(Schemas.eventsRawSchema))

    // Generate date range for backfill using sequence function
    val dateRange = spark.sql(s"""
      SELECT date_add(to_date('$startDate'), pos) as day
      FROM (
        SELECT posexplode(split(space(datediff(to_date('$endDate'), to_date('$startDate'))), ' ')) as (pos, x)
      )
    """)

    // Cross join users with date range to get all (user_id, day) combinations
    val users = eventsRaw.select("user_id").distinct()
    val userDays = users.crossJoin(dateRange)

    // Calculate features for each (user_id, day) combination
    val eventsWithDate = eventsRaw.withColumn("event_date", to_date(col("ts")))

    val features = userDays
      .join(
        eventsWithDate,
        userDays("user_id") === eventsWithDate("user_id") &&
          eventsWithDate("event_date") <= userDays("day") &&
          eventsWithDate("event_date") >= date_sub(userDays("day"), 30),
        "left"
      )
      .groupBy(userDays("user_id"), userDays("day"))
      .agg(
        // 7-day event count
        sum(
          when(
            datediff(userDays("day"), col("event_date")) <= 7 &&
              datediff(userDays("day"), col("event_date")) >= 0,
            1
          ).otherwise(0)
        ).as("event_count_7d"),
        // 30-day event count
        sum(
          when(
            datediff(userDays("day"), col("event_date")) <= 30 &&
              datediff(userDays("day"), col("event_date")) >= 0,
            1
          ).otherwise(0)
        ).as("event_count_30d"),
        // Days since last event
        min(
          when(
            col("event_date").isNotNull &&
              datediff(userDays("day"), col("event_date")) >= 0,
            datediff(userDays("day"), col("event_date"))
          ).otherwise(null)
        ).as("last_event_days_ago"),
        // Event type counts (simplified: count distinct types in window)
        countDistinct(col("event_type")).as("event_type_counts")
      )
      .select(
        col("user_id"),
        col("day"),
        col("event_count_7d"),
        col("event_count_30d"),
        col("last_event_days_ago"),
        col("event_type_counts").cast("string").as("event_type_counts")
      )

    // Write to Iceberg table
    platform.writer.insertOverwriteIcebergTable(
      features,
      outputTable,
      partitionBy = Seq("day")
    )
  }
}

