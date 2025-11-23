package com.example.featurestore.types

import org.apache.spark.sql.Dataset

/** Base trait for all feature store pipelines.
  *
  * Provides a common interface for pipeline execution and ensures
  * type safety across all pipeline implementations.
  *
  * Pipelines use SparkPlatformTrait directly via constructor parameters,
  * avoiding unnecessary abstraction layers.
  *
  * @tparam T The result type, must extend PipelineResult
  */
trait Pipeline[T <: PipelineResult] {
  /** Executes the pipeline.
    *
    * @return Option containing Dataset[T] if pipeline produces output, None otherwise
    */
  def execute(): PipelineResultType[T]
}

