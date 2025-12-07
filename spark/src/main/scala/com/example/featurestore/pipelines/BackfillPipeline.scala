package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import com.example.featurestore.types.{BackfillPipelineConfig, FeaturesDaily}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import platform.SparkPlatformTrait

/** Backfill pipeline: transforms events_raw → features_daily.
  *
  * Computes daily feature aggregations from raw events:
  *   - 7-day and 30-day event counts
  *   - Days since last event
  *   - Event type counts
  *
  * Data Transformation:
  *   1. Read events_raw (user_id, event_type, ts) 2. Generate date range from startDate to endDate
  *      3. Cross join distinct users × date range → ensures every (user, day) combination exists 4.
  *      Left join with events filtered by:
  *      - Same user_id
  *      - event_date <= feature day (point-in-time correctness, no data leakage)
  *      - event_date >= feature day - 30 days (optimization: only last 30 days needed)
  *      5. Group by (user_id, day) and aggregate:
  *      - event_count_7d: Count events in last 7 days (inclusive)
  *      - event_count_30d: Count events in last 30 days (inclusive)
  *      - last_event_days_ago: Days since most recent event (0 if event on feature day)
  *      - event_type_counts: Count of distinct event types in 30-day window
  *      6. Write to Iceberg table partitioned by day
  *
  * Use cross join to ensure complete coverage: every user has a row for every day in the date
  * range, even if they had no events. This is critical for point-in-time joins in downstream
  * pipelines, which need features for all historical dates.
  *
  * Sample Data Transformation (startDate="2024-01-01", endDate="2024-01-05"):
  *
  * Input (events_raw):
  * {{{
  * user_id | event_type | ts
  * --------|------------|-------------------
  * user1   | click      | 2024-01-01 10:00:00
  * user1   | purchase   | 2024-01-03 14:30:00
  * user1   | click      | 2024-01-05 16:45:00
  * }}}
  *
  * After cross join (userDays):
  * {{{
  * user_id | day
  * --------|----------
  * user1   | 2024-01-01
  * user1   | 2024-01-02
  * user1   | 2024-01-03
  * user1   | 2024-01-04
  * user1   | 2024-01-05
  * }}}
  *
  * Output (features_daily):
  * {{{
  * user_id | day         | event_count_7d | event_count_30d | last_event_days_ago | event_type_counts
  * --------|-------------|----------------|-----------------|---------------------|------------------
  * user1   | 2024-01-01  | 1              | 1               | 0                   | 1
  * user1   | 2024-01-02  | 1              | 1               | 1                   | 1
  * user1   | 2024-01-03  | 2              | 2               | 0                   | 2
  * user1   | 2024-01-04  | 2              | 2               | 1                   | 2
  * user1   | 2024-01-05  | 3              | 3               | 0                   | 2
  * }}}
  */
class BackfillPipeline(
  platform: SparkPlatformTrait,
  config: BackfillPipelineConfig
) {

  private val spark: SparkSession = platform.spark

  /** Executes the backfill pipeline.
    *
    * Writes features to Iceberg table. Returns None as backfill doesn't return data.
    *
    * @return
    *   None (pipeline writes to Iceberg table, no data returned)
    */
  def execute(): Option[FeaturesDaily] = {
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
    * @return
    *   Dataset[FeaturesDaily] with computed features
    */
  private def run(): Dataset[FeaturesDaily] = {
    import spark.implicits._

    // Read events_raw
    val eventsRaw = platform.fetcher.readParquet(
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
    val users    = eventsRaw.select("user_id").distinct()
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

}
