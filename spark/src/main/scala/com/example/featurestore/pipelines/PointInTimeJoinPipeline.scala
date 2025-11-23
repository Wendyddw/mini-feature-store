package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  *   val pipeline = new PointInTimeJoinPipeline(platform)
  *   val trainingData = pipeline.run(
  *     labelsPath = "s3://bucket/labels",
  *     featuresTable = "feature_store.features_daily",
  *     outputPath = "s3://bucket/training_data"
  *   )
  *   platform.stop()
  * }}}
  */
class PointInTimeJoinPipeline(platform: SparkPlatformTrait) {

  private val spark: SparkSession = platform.spark

  /** Runs the point-in-time join pipeline.
    *
    * @param labelsPath Path to labels table/data
    * @param featuresTable Iceberg table name for features_daily
    * @param outputPath Output path for joined training data
    * @return DataFrame with joined labels and features
    */
  def run(
      labelsPath: String,
      featuresTable: String,
      outputPath: String
  ): DataFrame = {
    import spark.implicits._

    // Read labels
    val labels = Fetchers.readParquet(spark, labelsPath, Some(Schemas.labelsSchema))
      .withColumn("as_of_date", to_date(col("as_of_ts")))

    // Read features_daily from Iceberg
    val features = spark.read
      .format("iceberg")
      .table(featuresTable)
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

    // Write output
    platform.writer.writeParquet(joined, outputPath, partitionBy = Seq("as_of_ts"))

    joined
  }
}

