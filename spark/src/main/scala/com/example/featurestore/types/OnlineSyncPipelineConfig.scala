package com.example.featurestore.types

/** Redis connection configuration.
  *
  * @param host Redis hostname
  * @param port Redis port
  *
  * @example
  * {{{
  *   val redisConfig = RedisConfig(host = "localhost", port = 6379)
  * }}}
  */
case class RedisConfig(
    host: String,
    port: Int
)

/** Configuration for OnlineSyncPipeline.
  *
  * @param featuresTable Iceberg table name for features_daily
  * @param redisConfig Redis connection configuration
  * @param hoursBack Number of hours back to sync (default: 24)
  *
  * @example
  * {{{
  *   val config = OnlineSyncPipelineConfig(
  *     featuresTable = "feature_store.features_daily",
  *     redisConfig = RedisConfig(host = "localhost", port = 6379),
  *     hoursBack = 24
  *   )
  * }}}
  */
case class OnlineSyncPipelineConfig(
    featuresTable: String,
    redisConfig: RedisConfig,
    hoursBack: Int = 24
)

