package com.example.featurestore.suit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.StructType
import platform.Fetchers

/** In-memory implementation of Fetchers for testing.
  *
  * Reads DataFrames from TestWriter's in-memory storage, making all data
  * operations happen in memory (dictionary) for fast, isolated tests.
  *
  * @param testWriter TestWriter instance that stores test data
  * @param spark SparkSession used to convert stored data back to DataFrames
  */
class TestFetcher(testWriter: TestWriter, spark: SparkSession) extends Fetchers {

  /** Retrieves stored data as a DataFrame.
    *
    * @param key The storage key (path or table name)
    * @return Option containing DataFrame if data exists, None otherwise
    */
  def getStoredData(key: String): Option[DataFrame] = {
    for {
      rows <- testWriter.db.get(key)
      schema <- testWriter.dbSchema.get(key)
    } yield {
      spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    }
  }

  def readParquet(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = {
    getStoredData(path) match {
      case Some(df) =>
        // Apply schema if provided
        schema match {
          case Some(s) => spark.createDataFrame(df.rdd, s)
          case None    => df
        }
      case None =>
        throw new IllegalArgumentException(s"Test data not found at path: $path")
    }
  }

  def readJson(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None
  ): DataFrame = {
    getStoredData(path) match {
      case Some(df) =>
        schema match {
          case Some(s) => spark.createDataFrame(df.rdd, s)
          case None    => df
        }
      case None =>
        throw new IllegalArgumentException(s"Test data not found at path: $path")
    }
  }

  def readCsv(
      spark: SparkSession,
      path: String,
      schema: Option[StructType] = None,
      header: Boolean = true,
      delimiter: String = ","
  ): DataFrame = {
    getStoredData(path) match {
      case Some(df) =>
        schema match {
          case Some(s) => spark.createDataFrame(df.rdd, s)
          case None    => df
        }
      case None =>
        throw new IllegalArgumentException(s"Test data not found at path: $path")
    }
  }

  def readIcebergTable(
      spark: SparkSession,
      tableName: String
  ): DataFrame = {
    getStoredData(tableName) match {
      case Some(df) => df
      case None =>
        throw new IllegalArgumentException(s"Test data not found in table: $tableName")
    }
  }
}

