package com.example.featurestore.suit

import org.apache.spark.sql.{DataFrame, Row, SparkSession, StructType}
import platform.Writers
import scala.collection.mutable

/** In-memory implementation of Writers for testing.
  *
  * Stores DataFrames as materialized Row data with their schemas, making them
  * independent of the SparkSession lifecycle. This ensures test data remains
  * valid even if the SparkSession is recreated.
  *
  * @param spark SparkSession used to convert stored data back to DataFrames
  */
class TestWriter(spark: SparkSession) extends Writers {
  // Store materialized data and schema separately for better test isolation
  private val db: mutable.Map[String, Seq[Row]] = mutable.Map.empty
  private val dbSchema: mutable.Map[String, StructType] = mutable.Map.empty

  /** Retrieves stored data as a DataFrame.
    *
    * @param key The storage key (path or table name)
    * @return Option containing DataFrame if data exists, None otherwise
    */
  def getStoredData(key: String): Option[DataFrame] = {
    for {
      rows <- db.get(key)
      schema <- dbSchema.get(key)
    } yield {
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }
  }

  /** Returns all storage keys that have been written to. */
  def getAllStoredKeys: Set[String] = db.keys.toSet

  /** Clears all stored data and schemas. */
  def clearStorage(): Unit = {
    db.clear()
    dbSchema.clear()
  }

  /** Stores DataFrame data by materializing it immediately.
    * This ensures data is independent of the original DataFrame's SparkSession.
    */
  private def storeDataFrame(key: String, df: DataFrame, mode: String): Unit = {
    // Materialize the DataFrame immediately to avoid lazy evaluation issues
    val rows = df.collect().toSeq
    val schema = df.schema

    mode match {
      case "overwrite" | "error" =>
        db(key) = rows
        dbSchema(key) = schema
      case "append" =>
        val existingRows = db.getOrElse(key, Seq.empty)
        db(key) = existingRows ++ rows
        // Schema should match, but we'll keep the original
        if (!dbSchema.contains(key)) {
          dbSchema(key) = schema
        }
      case "ignore" =>
        if (!db.contains(key)) {
          db(key) = rows
          dbSchema(key) = schema
        }
      case _ =>
        db(key) = rows
        dbSchema(key) = schema
    }
  }

  override def writeParquet(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def writeJson(
      df: DataFrame,
      path: String,
      mode: String = "overwrite"
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def writeCsv(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      header: Boolean = true,
      delimiter: String = ","
  ): Unit = {
    storeDataFrame(path, df, mode)
  }

  override def insertOverwriteIcebergTable(
      df: DataFrame,
      tableName: String,
      partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    storeDataFrame(tableName, df, "overwrite")
  }
}

