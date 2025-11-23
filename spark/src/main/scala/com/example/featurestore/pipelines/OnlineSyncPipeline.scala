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
    * @return None (pipeline writes to Redis, no Dataset returned)
    */
  def execute(): Option[Dataset[FeaturesDaily]] = {
    // Validate configuration
    validateConfig()

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
    val redis = new Jedis(config.redisConfig.host, config.redisConfig.port)
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

  /** Validates the pipeline configuration.
    *
    * @throws IllegalArgumentException if configuration is invalid
    */
  private def validateConfig(): Unit = {
    require(config.featuresTable.nonEmpty, "featuresTable cannot be empty")
    require(config.redisConfig.host.nonEmpty, "Redis host cannot be empty")
    require(
      config.redisConfig.port > 0 && config.redisConfig.port <= 65535,
      s"Redis port must be between 1 and 65535, got ${config.redisConfig.port}"
    )
    require(config.hoursBack > 0, s"hoursBack must be positive, got ${config.hoursBack}")
  }
}

