package com.example.featurestore.types

import org.apache.spark.sql.Dataset
import platform.SparkPlatformTrait

/** Base trait for all feature store pipelines.
  *
  * Provides a common interface for pipeline execution and ensures
  * type safety across all pipeline implementations.
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

/** Trait for pipelines that require a SparkPlatform.
  *
  * All feature store pipelines need access to Spark session and writers.
  *
  * @tparam T The result type, must extend PipelineResult
  */
trait PlatformPipeline[T <: PipelineResult] extends Pipeline[T] {
  /** The Spark platform providing session and writer access */
  val platform: SparkPlatformTrait
}

