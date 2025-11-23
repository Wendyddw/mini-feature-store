package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import com.example.featurestore.types.{
  PointInTimeJoinPipelineConfig,
  Pipeline,
  TrainingData
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import platform.{Fetchers, SparkPlatformTrait}

/** Point-in-time join pipeline: joins labels with features_daily.
  *
  * Performs temporal join ensuring no data leakage:
  * - Joins labels with features where feature_day <= as_of_ts_day
  * - Selects the latest feature snapshot for each label
  *
  * This is critical for ML training to ensure features used are only from
  * time points before or at the label timestamp.
  *
  * Common use patterns:
  * {{{
  *   val platform = PlatformProvider.createLocal("point-in-time-join")
  *   val config = PointInTimeJoinPipelineConfig(
  *     labelsPath = "s3://bucket/labels",
  *     featuresTable = "feature_store.features_daily",
  *     outputPath = "s3://bucket/training_data"
  *   )
  *   val pipeline = new PointInTimeJoinPipeline(platform, config)
  *   val trainingData = pipeline.execute().get
  *   platform.stop()
  * }}}
  */
class PointInTimeJoinPipeline(
    platform: SparkPlatformTrait,
    config: PointInTimeJoinPipelineConfig
) extends Pipeline[TrainingData] {

  private val spark: SparkSession = platform.spark

  /** Executes the point-in-time join pipeline.
    *
    * @return Some(Dataset[TrainingData]) with joined labels and features
    */
  override def execute(): Option[Dataset[TrainingData]] = {
    // Validate configuration
    validateConfig()

    val trainingData = run()
    // Write output
    platform.writer.writeParquet(
      trainingData.toDF(),
      config.outputPath,
      partitionBy = config.partitionBy
    )
    Some(trainingData)
  }

  /** Runs the point-in-time join pipeline.
    *
    * Internal implementation method.
    *
    * @return Dataset[TrainingData] with joined labels and features
    */
  private def run(): Dataset[TrainingData] = {
    import spark.implicits._

    // Read labels
    val labels = Fetchers.readParquet(spark, config.labelsPath, Some(Schemas.labelsSchema))
      .withColumn("as_of_date", to_date(col("as_of_ts")))

    // Read features_daily from Iceberg
    val features = spark.read
      .format("iceberg")
      .table(config.featuresTable)
      .withColumn("feature_date", col("day"))

    // Point-in-time join: feature_day <= as_of_ts_day
    // Then select the latest feature snapshot for each label
    val joined = labels
      .join(
        features,
        labels("user_id") === features("user_id") &&
          features("feature_date") <= labels("as_of_date"),
        "left"
      )
      .withColumn(
        "rank",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy(labels("user_id"), labels("as_of_ts"))
            .orderBy(features("feature_date").desc)
        )
      )
      .filter(col("rank") === 1)
      .drop("rank", "feature_date", "as_of_date")
      .select(
        col("user_id"),
        col("label"),
        col("as_of_ts"),
        col("day"),
        col("event_count_7d"),
        col("event_count_30d"),
        col("last_event_days_ago"),
        col("event_type_counts")
      )
      .as[TrainingData]

    joined
  }

  /** Validates the pipeline configuration.
    *
    * @throws IllegalArgumentException if configuration is invalid
    */
  private def validateConfig(): Unit = {
    require(config.labelsPath.nonEmpty, "labelsPath cannot be empty")
    require(config.featuresTable.nonEmpty, "featuresTable cannot be empty")
    require(config.outputPath.nonEmpty, "outputPath cannot be empty")
    require(
      config.partitionBy.nonEmpty,
      "partitionBy cannot be empty (at least one partition column required)"
    )
  }
}

