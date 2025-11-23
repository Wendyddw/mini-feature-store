package com.example.featurestore.pipelines

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import platform.SparkPlatformTrait
import redis.clients.jedis.Jedis

/** Online sync pipeline: syncs features_daily to Redis for low-latency serving.
  *
  * Syncs the last 24 hours of features to Redis, keyed by user_id.
  * This enables fast feature serving for online inference.
  *
  * Common use patterns:
  * {{{
  *   val platform = PlatformProvider.createLocal("online-sync")
  *   val pipeline = new OnlineSyncPipeline(platform, "localhost", 6379)
  *   pipeline.sync(
  *     featuresTable = "feature_store.features_daily",
  *     hoursBack = 24
  *   )
  *   platform.stop()
  * }}}
  */
class OnlineSyncPipeline(platform: SparkPlatformTrait, redisHost: String, redisPort: Int) {

  private val spark: SparkSession = platform.spark

  /** Syncs recent features to Redis.
    *
    * @param featuresTable Iceberg table name for features_daily
    * @param hoursBack Number of hours back to sync (default: 24)
    */
  def sync(featuresTable: String, hoursBack: Int = 24): Unit = {
    import spark.implicits._

    // Read recent features from Iceberg
    val cutoffDate = date_sub(current_date(), 1) // Last 24 hours
    val recentFeatures = spark.read
      .format("iceberg")
      .table(featuresTable)
      .filter(col("day") >= cutoffDate)
      .orderBy(col("day").desc)

    // Get latest feature snapshot per user
    val latestFeatures = recentFeatures
      .withColumn(
        "rank",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy("user_id")
            .orderBy(col("day").desc)
        )
      )
      .filter(col("rank") === 1)
      .drop("rank")

    // Convert to JSON and write to Redis
    val redis = new Jedis(redisHost, redisPort)
    try {
      latestFeatures.collect().foreach { row =>
        val userId = row.getAs[String]("user_id")
        val day = row.getAs[java.sql.Date]("day").toString
        val featuresJson = Map(
          "day" -> day,
          "event_count_7d" -> Option(row.getAs[Long]("event_count_7d")).map(_.toString).getOrElse("null"),
          "event_count_30d" -> Option(row.getAs[Long]("event_count_30d")).map(_.toString).getOrElse("null"),
          "last_event_days_ago" -> Option(row.getAs[Int]("last_event_days_ago")).map(_.toString).getOrElse("null"),
          "event_type_counts" -> Option(row.getAs[String]("event_type_counts")).getOrElse("null")
        ).map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")

        val key = s"features:${userId}"
        redis.set(key, featuresJson)
      }
    } finally {
      redis.close()
    }
  }
}

