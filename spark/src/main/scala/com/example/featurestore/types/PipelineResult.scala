package com.example.featurestore.types

import org.apache.spark.sql.Dataset

/** Trait for pipeline results that include timestamp information.
  *
  * Ensures type safety for temporal data in feature store pipelines.
  */
trait Timestamped {
  /** Timestamp associated with this record */
  def timestamp: java.sql.Timestamp
}

/** Trait for pipeline results that include date information.
  *
  * Used for daily aggregated features.
  */
trait Dated {
  /** Date associated with this record */
  def date: java.sql.Date
}

/** Trait for results that can be used for training.
  *
  * Training data must include labels and features.
  */
trait Trainable {
  /** Label value for training */
  def label: Double
}

/** Base trait for all pipeline result types.
  *
  * Provides a common interface for pipeline outputs.
  */
trait PipelineResult

/** Type alias for typed pipeline results.
  *
  * Pipelines return Option[Dataset[T]] where T extends PipelineResult.
  */
type PipelineResultType[T <: PipelineResult] = Option[Dataset[T]]

