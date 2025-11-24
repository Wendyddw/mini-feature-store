package com.example.featurestore.pipelines

import com.example.featurestore.types.{
  FeaturesDaily,
  OnlineSyncPipelineConfig
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
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
  *   val config = OnlineSyncPipelineConfig(
  *     featuresTable = "feature_store.features_daily",
  *     redisConfig = RedisConfig(host = "localhost", port = 6379),
  *     hoursBack = 24
  *   )
  *   val pipeline = new OnlineSyncPipeline(platform, config)
  *   pipeline.execute()
  *   platform.stop()
  * }}}
  */
class OnlineSyncPipeline(
    platform: SparkPlatformTrait,
    config: OnlineSyncPipelineConfig
) {

  private val spark: SparkSession = platform.spark

  /** Executes the online sync pipeline.
    *
    * Syncs features to Redis. Returns None as sync doesn't return data.
    *
    * @return None (pipeline writes to Redis, no data returned)
    */
  def execute(): Option[FeaturesDaily] = {
    sync()
    None
  }

  /** Syncs recent features to Redis.
    *
    * Internal implementation method.
    */
  private def sync(): Unit = {
    import spark.implicits._

    // Read recent features from Iceberg
    val cutoffDate = date_sub(current_date(), config.hoursBack / 24)
    val recentFeatures = spark.read
      .format("iceberg")
      .table(config.featuresTable)
      .filter(col("day") >= cutoffDate)
      .orderBy(col("day").desc)
      .as[FeaturesDaily]

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
      .as[FeaturesDaily]

    // Convert to JSON and write to Redis
    val redis = new Jedis(config.redisConfig.host, config.redisConfig.port)
    try {
      latestFeatures.collect().foreach { featuresDaily =>
        val userId = featuresDaily.user_id
        val day = featuresDaily.day.toString
        val featuresJson = Map(
          "day" -> day,
          "event_count_7d" -> featuresDaily.event_count_7d.map(_.toString).getOrElse("null"),
          "event_count_30d" -> featuresDaily.event_count_30d.map(_.toString).getOrElse("null"),
          "last_event_days_ago" -> featuresDaily.last_event_days_ago.map(_.toString).getOrElse("null"),
          "event_type_counts" -> featuresDaily.event_type_counts.getOrElse("null")
        ).map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")

        val key = s"features:${userId}"
        redis.set(key, featuresJson)
      }
    } finally {
      redis.close()
    }
  }

}

