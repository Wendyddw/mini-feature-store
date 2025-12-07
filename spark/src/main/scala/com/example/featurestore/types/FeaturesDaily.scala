package com.example.featurestore.types

import java.sql.Date

/** Daily features from features_daily table.
  *
  * Represents daily aggregated features per user:
  *   - event_count_7d: count of events in last 7 days
  *   - event_count_30d: count of events in last 30 days
  *   - last_event_days_ago: days since last event
  *   - event_type_counts: counts by event type (as JSON string)
  */
case class FeaturesDaily(
  user_id: String,
  day: Date,
  event_count_7d: Option[Long],
  event_count_30d: Option[Long],
  last_event_days_ago: Option[Int],
  event_type_counts: Option[String]
)
