package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import com.example.featurestore.types.{
  BackfillPipelineConfig,
  FeaturesDaily
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
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
  *   val config = BackfillPipelineConfig(
  *     eventsRawPath = "s3://bucket/events_raw",
  *     outputTable = "feature_store.features_daily",
  *     startDate = "2024-01-01",
  *     endDate = "2024-12-31"
  *   )
  *   val pipeline = new BackfillPipeline(platform, config)
  *   pipeline.execute()
  *   platform.stop()
  * }}}
  */
class BackfillPipeline(
    platform: SparkPlatformTrait,
    config: BackfillPipelineConfig
) {

  private val spark: SparkSession = platform.spark

  /** Executes the backfill pipeline.
    *
    * @return None (pipeline writes to Iceberg table, no Dataset returned)
    */
  def execute(): Option[Dataset[FeaturesDaily]] = {
    // Validate configuration
    validateConfig()

    val features = run()
    // Write to Iceberg table
    platform.writer.insertOverwriteIcebergTable(
      features.toDF(),
      config.outputTable,
      partitionBy = config.partitionBy
    )
    None // Backfill doesn't return data, it writes to table
  }

  /** Runs the backfill pipeline.
    *
    * Internal implementation method.
    *
    * @return Dataset[FeaturesDaily] with computed features
    */
  private def run(): Dataset[FeaturesDaily] = {
    import spark.implicits._

    // Read events_raw
    val eventsRaw = Fetchers.readParquet(
      spark,
      config.eventsRawPath,
      Some(Schemas.eventsRawSchema)
    )

    // Generate date range for backfill using sequence function
    val dateRange = spark.sql(s"""
      SELECT date_add(to_date('${config.startDate}'), pos) as day
      FROM (
        SELECT posexplode(split(space(datediff(to_date('${config.endDate}'), to_date('${config.startDate}'))), ' ')) as (pos, x)
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
      .as[FeaturesDaily]

    features
  }

  /** Validates the pipeline configuration.
    *
    * @throws IllegalArgumentException if configuration is invalid
    */
  private def validateConfig(): Unit = {
    require(config.eventsRawPath.nonEmpty, "eventsRawPath cannot be empty")
    require(config.outputTable.nonEmpty, "outputTable cannot be empty")
    require(config.startDate.nonEmpty, "startDate cannot be empty")
    require(config.endDate.nonEmpty, "endDate cannot be empty")
    require(
      config.partitionBy.nonEmpty,
      "partitionBy cannot be empty (at least one partition column required)"
    )

    // Validate date format (basic check)
    try {
      java.sql.Date.valueOf(config.startDate)
      java.sql.Date.valueOf(config.endDate)
    } catch {
      case _: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"Invalid date format. Expected YYYY-MM-DD, got startDate=${config.startDate}, endDate=${config.endDate}"
        )
    }

    // Validate date range
    val start = java.sql.Date.valueOf(config.startDate)
    val end = java.sql.Date.valueOf(config.endDate)
    require(
      !start.after(end),
      s"startDate (${config.startDate}) must be before or equal to endDate (${config.endDate})"
    )
  }
}

