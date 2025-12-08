package com.example.featurestore

import com.example.featurestore.pipelines.{
  BackfillPipeline,
  OnlineSyncPipeline,
  PointInTimeJoinPipeline
}
import com.example.featurestore.types.{
  BackfillPipelineConfig,
  OnlineSyncPipelineConfig,
  PointInTimeJoinPipelineConfig,
  RedisConfig
}
import platform.PlatformProvider

/** Main application entry point for feature store pipelines.
  *
  * Supports three pipeline modes:
  *   - backfill: events_raw → features_daily
  *   - point-in-time-join: labels + features_daily → training_data
  *   - online-sync: features_daily → Redis
  *
  * Usage:
  * {{{
  *   spark-submit --class com.example.featurestore.App mini-feature-store-spark.jar \
  *     backfill \
  *     --events-raw-path s3://bucket/events_raw \
  *     --output-table feature_store.features_daily \
  *     --start-date 2024-01-01 \
  *     --end-date 2024-12-31
  * }}}
  */
object App {
  def main(args: Array[String]): Unit = {
    // Workaround for Java 17+ compatibility: configure Hadoop to avoid Subject API
    // Subject.getSubject() was removed in Java 17+, so we configure Hadoop to use simple auth
    System.setProperty("hadoop.security.authentication", "simple")
    System.setProperty("java.security.auth.login.config", "")

    if (args.length < 1) {
      println(
        """Usage: <pipeline> [options]
          |
          |Pipelines:
          |  backfill --events-raw-path <path> --output-table <table> --start-date <date> --end-date <date>
          |  point-in-time-join --labels-path <path> --features-table <table> --output-path <path>
          |  online-sync --features-table <table> --redis-host <host> --redis-port <port> [--hours-back <hours>]
          |""".stripMargin
      )
      System.exit(1)
    }

    // Use local mode for local development (sbt runMain), or when SPARK_MASTER is not set
    // For production, set SPARK_MASTER environment variable or use spark-submit
    val master = Option(System.getenv("SPARK_MASTER")).orElse(Option(System.getProperty("spark.master")))

    // Spark configuration for Iceberg with S3/MinIO storage
    val sparkConfig = getSparkConfigForIceberg()

    val platform = if (master.isEmpty) {
      // Local development mode
      PlatformProvider.createLocal(
        appName = s"feature-store-${args(0)}",
        config = sparkConfig
      )
    } else {
      // Production mode with specified master
      PlatformProvider.create(
        appName = s"feature-store-${args(0)}",
        master = master,
        config = sparkConfig
      )
    }

    try
      args(0) match {
        case "backfill" =>
          val options = parseArgs(args.tail)
          val config = BackfillPipelineConfig(
            eventsRawPath = options("events-raw-path"),
            outputTable = options("output-table"),
            startDate = options("start-date"),
            endDate = options("end-date")
          )
          val pipeline = new BackfillPipeline(platform, config)
          pipeline.execute()

        case "point-in-time-join" =>
          val options = parseArgs(args.tail)
          val config = PointInTimeJoinPipelineConfig(
            labelsPath = options("labels-path"),
            featuresTable = options("features-table"),
            outputPath = options("output-path")
          )
          val pipeline = new PointInTimeJoinPipeline(platform, config)
          pipeline.execute()

        case "online-sync" =>
          val options = parseArgs(args.tail)
          val config = OnlineSyncPipelineConfig(
            featuresTable = options("features-table"),
            redisConfig = RedisConfig(
              host = options("redis-host"),
              port = options("redis-port").toInt
            ),
            hoursBack = options.get("hours-back").map(_.toInt).getOrElse(24)
          )
          val pipeline = new OnlineSyncPipeline(platform, config)
          pipeline.execute()

        case _ =>
          println(s"Unknown pipeline: ${args(0)}")
          System.exit(1)
      }
    finally
      platform.stop()
  }

  /** Returns Spark configuration for Iceberg with S3/MinIO storage.
    *
    * Combines S3A filesystem configuration (for MinIO/S3 access) with Iceberg catalog
    * configuration. This configuration is used for both local development and production.
    *
    * @return Map of Spark configuration properties
    */
  private def getSparkConfigForIceberg(): Map[String, String] = {
    // S3A filesystem configuration for MinIO/S3 access
    val s3Config = Map(
      "spark.hadoop.fs.s3a.endpoint" -> "http://localhost:9000",
      "spark.hadoop.fs.s3a.access.key" -> "minioadmin",
      "spark.hadoop.fs.s3a.secret.key" -> "minioadmin",
      "spark.hadoop.fs.s3a.path.style.access" -> "true",
      "spark.hadoop.fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
      "spark.hadoop.fs.s3a.connection.ssl.enabled" -> "false"
    )

    // Iceberg catalog configuration (hadoop catalog type, no Hive Metastore required)
    val icebergConfig = Map(
      "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
      "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
      "spark.sql.catalog.spark_catalog.type" -> "hadoop",
      "spark.sql.catalog.spark_catalog.warehouse" -> "s3a://warehouse/"
    )

    s3Config ++ icebergConfig
  }

  private def parseArgs(args: Array[String]): Map[String, String] =
    args
      .sliding(2, 2)
      .map { case Array(key, value) =>
        key.stripPrefix("--") -> value
      }
      .toMap
}
