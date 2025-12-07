package com.example.featurestore.types

/** Configuration for BackfillPipeline.
  *
  * @param eventsRawPath
  *   Path to events_raw table/data (Parquet format)
  * @param outputTable
  *   Output Iceberg table name (e.g., "feature_store.features_daily")
  * @param startDate
  *   Start date for backfill (YYYY-MM-DD format)
  * @param endDate
  *   End date for backfill (YYYY-MM-DD format)
  * @param partitionBy
  *   Columns to partition the output table by (default: Seq("day"))
  *
  * @example
  *   {{{
  *   val config = BackfillPipelineConfig(
  *     eventsRawPath = "s3://bucket/events_raw",
  *     outputTable = "feature_store.features_daily",
  *     startDate = "2024-01-01",
  *     endDate = "2024-12-31"
  *   )
  *   }}}
  */
case class BackfillPipelineConfig(
  eventsRawPath: String,
  outputTable: String,
  startDate: String,
  endDate: String,
  partitionBy: Seq[String] = Seq("day")
)
