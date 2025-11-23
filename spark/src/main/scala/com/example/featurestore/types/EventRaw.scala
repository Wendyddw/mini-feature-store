package com.example.featurestore.types

import java.sql.Timestamp

/** Raw event from events_raw table.
  *
  * Represents a single event with user_id, event_type, and timestamp.
  */
case class EventRaw(
    user_id: String,
    event_type: String,
    ts: Timestamp
)

