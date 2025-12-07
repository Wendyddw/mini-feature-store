package com.example.featurestore.pipelines

import com.example.featurestore.domain.Schemas
import com.example.featurestore.types.{PointInTimeJoinPipelineConfig, TrainingData}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import platform.SparkPlatformTrait

/** Point-in-time join pipeline: joins labels with features_daily.
  *
  * Performs temporal join ensuring no data leakage:
  *   - Joins labels with features where feature_day <= as_of_ts_day
  *   - Selects the latest feature snapshot for each label
  *
  * This is critical for ML training to ensure features used are only from time points before or at
  * the label timestamp.
  *
  * Data Transformation:
  *   1. Read labels (user_id, label, as_of_ts) and features_daily (user_id, day, features...) 2.
  *      Join on user_id where feature_date <= as_of_date (point-in-time correctness) 3. Rank
  *      features by feature_date descending per (user_id, as_of_ts) 4. Filter to rank=1 (latest
  *      feature snapshot) 5. Select final columns and write to Parquet
  *
  * Sample Data Transformation:
  *
  * Input (labels):
  * {{{
  * user_id | label | as_of_ts
  * --------|-------|-------------------
  * user1   | 1.0   | 2024-01-03 12:00:00
  * }}}
  *
  * Input (features_daily):
  * {{{
  * user_id | day         | event_count_7d
  * --------|-------------|---------------
  * user1   | 2024-01-03  | 2
  * user1   | 2024-01-04  | 5  (future, excluded)
  * }}}
  *
  * Output (training_data):
  * {{{
  * user_id | label | as_of_ts          | day         | event_count_7d
  * --------|-------|-------------------|-------------|---------------
  * user1   | 1.0   | 2024-01-03 12:00  | 2024-01-03  | 2
  * }}}
  *
  * Key point: For user1's label at 2024-01-03, we use features from 2024-01-03 (latest on or before
  * the label timestamp). Features from 2024-01-04 are excluded (feature_date > as_of_date),
  * preventing data leakage.
  */
class PointInTimeJoinPipeline(
  platform: SparkPlatformTrait,
  config: PointInTimeJoinPipelineConfig
) {

  private val spark: SparkSession = platform.spark

  /** Executes the point-in-time join pipeline.
    *
    * @return
    *   Some(Seq[TrainingData]) if data exists, None otherwise
    */
  def execute(): Option[Seq[TrainingData]] = {
    val trainingData = run()

    // Check if dataset is empty
    val collected = trainingData.collect()
    if (collected.isEmpty) {
      None
    } else {
      // Write output
      platform.writer.writeParquet(
        trainingData.toDF(),
        config.outputPath,
        partitionBy = config.partitionBy
      )
      Some(collected.toSeq)
    }
  }

  /** Runs the point-in-time join pipeline.
    *
    * Internal implementation method.
    *
    * @return
    *   Dataset[TrainingData] with joined labels and features
    */
  private def run(): Dataset[TrainingData] = {
    import spark.implicits._

    // Read labels
    val labels = platform.fetcher
      .readParquet(spark, config.labelsPath, Some(Schemas.labelsSchema))
      .withColumn("as_of_date", to_date(col("as_of_ts")))

    // Read features_daily from Iceberg
    val features = platform.fetcher
      .readIcebergTable(spark, config.featuresTable)
      .withColumn("feature_date", col("day"))

    // Step 1: Point-in-time join - feature_day <= as_of_ts_day
    val joined = labels
      .join(
        features,
        labels("user_id") === features("user_id") &&
          features("feature_date") <= labels("as_of_date"),
        "left"
      )
      .drop(features("user_id"))

    // Step 2: Ranking - select the latest feature snapshot for each label
    val ranked = joined
      .withColumn(
        "rank",
        row_number().over(
          org.apache.spark.sql.expressions.Window
            .partitionBy(col("user_id"), col("as_of_ts"))
            .orderBy(col("feature_date").desc)
        )
      )
      .filter(col("rank") === 1)

    // Step 3: Select - choose final columns and convert to TrainingData
    val trainingData = ranked
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

    trainingData
  }

}
