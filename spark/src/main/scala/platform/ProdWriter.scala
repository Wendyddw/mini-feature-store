package platform

import org.apache.spark.sql.DataFrame

/** Production implementation of Writers trait.
  *
  * Writes DataFrames to real storage systems (Parquet, JSON, CSV, Iceberg tables). This is the
  * default writer used in production environments.
  *
  * Common use patterns:
  * {{{
  *   // Automatically used by PlatformProvider by default
  *   val platform = PlatformProvider.createLocal("my-app")
  *   platform.writer.writeParquet(df, "s3://bucket/output.parquet")
  *
  *   // Explicitly create for custom usage
  *   val writer = new ProdWriter()
  *   writer.insertOverwriteIcebergTable(df, "db.features", partitionBy = Seq("date"))
  * }}}
  */
class ProdWriter extends Writers {

  override def writeParquet(
    df: DataFrame,
    path: String,
    mode: String = "overwrite",
    partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    val writer = df.write.mode(mode)
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*).parquet(path)
    } else {
      writer.parquet(path)
    }
  }

  override def writeJson(
    df: DataFrame,
    path: String,
    mode: String = "overwrite"
  ): Unit =
    df.write.mode(mode).json(path)

  override def writeCsv(
    df: DataFrame,
    path: String,
    mode: String = "overwrite",
    header: Boolean = true,
    delimiter: String = ","
  ): Unit =
    df.write
      .mode(mode)
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(path)

  override def insertOverwriteIcebergTable(
    df: DataFrame,
    tableName: String,
    partitionBy: Seq[String] = Seq.empty
  ): Unit = {
    val writer = df.write
      .format("iceberg")
      .mode("overwrite")
    if (partitionBy.nonEmpty) {
      writer.partitionBy(partitionBy: _*).saveAsTable(tableName)
    } else {
      writer.saveAsTable(tableName)
    }
  }
}
