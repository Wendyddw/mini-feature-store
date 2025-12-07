package com.example.featurestore.types

/** Configuration for PointInTimeJoinPipeline.
  *
  * @param labelsPath
  *   Path to labels table/data (Parquet format)
  * @param featuresTable
  *   Iceberg table name for features_daily
  * @param outputPath
  *   Output path for joined training data (Parquet format)
  * @param partitionBy
  *   Columns to partition the output by (default: Seq("as_of_ts"))
  *
  * @example
  *   {{{
  *   val config = PointInTimeJoinPipelineConfig(
  *     labelsPath = "s3://bucket/labels",
  *     featuresTable = "feature_store.features_daily",
  *     outputPath = "s3://bucket/training_data"
  *   )
  *   }}}
  */
case class PointInTimeJoinPipelineConfig(
  labelsPath: String,
  featuresTable: String,
  outputPath: String,
  partitionBy: Seq[String] = Seq("as_of_ts")
)
