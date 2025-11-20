package platform

import org.apache.spark.sql.DataFrame

/** Trait defining the interface for data writing operations.
  *
  * Provides abstraction for writing DataFrames to various storage formats
  * and systems. Implementations include ProdWriter (real storage) and
  * TestWriter (in-memory for testing).
  *
  * Common use patterns:
  * {{{
  *   // Get writer from platform
  *   val platform = PlatformProvider.createLocal("my-app")
  *   val writer = platform.writer
  *
  *   // Write Parquet files
  *   writer.writeParquet(df, "output.parquet", partitionBy = Seq("date", "region"))
  *
  *   // Write to Iceberg table
  *   writer.insertOverwriteIcebergTable(df, "db.feature_table", partitionBy = Seq("date"))
  *
  *   // Write JSON/CSV
  *   writer.writeJson(df, "output.json")
  *   writer.writeCsv(df, "output.csv", header = true)
  * }}}
  */
trait Writers {
  /** Writes a DataFrame to Parquet format.
    *
    * @param df DataFrame to write
    * @param path Output path for Parquet files
    * @param mode Write mode: "overwrite", "append", "ignore", "error" (default: "overwrite")
    * @param partitionBy Column names to partition by (default: empty, no partitioning)
    */
  def writeParquet(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      partitionBy: Seq[String] = Seq.empty
  ): Unit

  /** Writes a DataFrame to JSON format.
    *
    * @param df DataFrame to write
    * @param path Output path for JSON files
    * @param mode Write mode: "overwrite", "append", "ignore", "error" (default: "overwrite")
    */
  def writeJson(
      df: DataFrame,
      path: String,
      mode: String = "overwrite"
  ): Unit

  /** Writes a DataFrame to CSV format.
    *
    * @param df DataFrame to write
    * @param path Output path for CSV files
    * @param mode Write mode: "overwrite", "append", "ignore", "error" (default: "overwrite")
    * @param header Whether to include header row (default: true)
    * @param delimiter Field delimiter (default: ",")
    */
  def writeCsv(
      df: DataFrame,
      path: String,
      mode: String = "overwrite",
      header: Boolean = true,
      delimiter: String = ","
  ): Unit

  /** Writes a DataFrame to an Iceberg table using INSERT OVERWRITE semantics.
    *
    * This method performs an atomic overwrite of the table, replacing all existing data.
    * For feature stores, this is commonly used for point-in-time feature snapshots.
    *
    * @param df DataFrame to write
    * @param tableName Fully qualified table name (e.g., "db.feature_table")
    * @param partitionBy Column names to partition the table by (default: empty)
    *
    * @example
    * {{{
    *   // Write features partitioned by date
    *   writer.insertOverwriteIcebergTable(
    *     df = featureDF,
    *     tableName = "feature_store.user_features",
    *     partitionBy = Seq("feature_date")
    *   )
    * }}}
    */
  def insertOverwriteIcebergTable(
      df: DataFrame,
      tableName: String,
      partitionBy: Seq[String] = Seq.empty
  ): Unit
}
