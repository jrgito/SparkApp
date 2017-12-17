package com.jrgv89.spark.utils.datatable

import com.jrgv89.spark.utils.datatable.exceptions.{MandatoryKeyNotFound, MandatoryKeyNotFoundErrors}
import com.jrgv89.spark.utils.datatable.tableOps.TableOps
import com.jrgv89.spark.utils.utils.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataTable(val name: String,
                val inputPath: String,
                val outputPath: String,
                val updatable: Boolean = false,
                val isReadOnly: Boolean = false,
                val format: String,
                val writeMode: String,
                val pks: Seq[String],
                val partitions: Boolean,
                val partitionColumns: Seq[String],
                val properties: Option[Map[String, String]] = None,
                val preserveSchema: Boolean = true,
                var schema: Option[StructType] = None
               ) extends TableOps {

  def read(spark: SparkSession): DataFrame = readTable(spark, this)

  def write(df: DataFrame): Unit = writeTable(df, this)

  def writeWithTS(df: DataFrame, timestampColumn: String): Unit = writeTableWithTS(df,this, timestampColumn)

  def update(df: DataFrame): Unit = updateTable(df, this)

  def updateWithTS(df: DataFrame, timestampColumn: String): Unit = updateTableWithTS(df, this, timestampColumn)

  def delete(spark: SparkSession): Unit = deleteTable(spark, this)

  def readPartition(spark: SparkSession, partitionKey: String, partitionValue: Any): DataFrame =
    readPartition(spark, this, partitionKey: String, partitionValue: Any)

  def readDeepPartition(spark: SparkSession, partitions: Seq[(String, Any)]): DataFrame =
    readDeepPartition(spark, this, partitions)

  def writePartition(df: DataFrame, partitionKey: String, partitionValue: Any): Unit = writePartition(df, this, partitionKey, partitionValue)

  def writePartitionWithTS(df: DataFrame, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit =
    writePartitionWithTS(df, this, partitionKey, partitionValue, timestampColumn)

  def writeDeepPartition(df: DataFrame, partitions: Seq[(String, Any)]): Unit = writeDeepPartition(df, this, partitions)

  def writeDeepPartitionWithTS(df: DataFrame, partitions: Seq[(String, Any)], timestampColumn: String): Unit =
    writeDeepPartitionWithTS(df, this, partitions, timestampColumn)

  def updatePartition(df: DataFrame, partitionKey: String, partitionValue: Any): Unit = updatePartition(df, this, partitionKey, partitionValue)

  def updatePartitionWithTS(df: DataFrame, partitionKey: String, partitionValue: Any, timestampColumn: String): Unit =
    updatePartitionWithTS(df, this, partitionKey, partitionValue, timestampColumn)

  def updateDeepPartition(df: DataFrame, partitions: Seq[(String, Any)]): Unit = updateDeepPartition(df, this, partitions)

  def updateDeepPartitionWithTS(df: DataFrame, partitions: Seq[(String, Any)], timestampColumn: String): Unit =
    updateDeepPartitionWithTS(df, this, partitions, timestampColumn)

  def deletePartition(spark: SparkSession,partitionKey: String, partitionValue: Any): Unit = deletePartition(spark,this, partitionKey, partitionValue)

  def deleteDeepPartition(spark: SparkSession,partitions: Seq[(String, Any)]): Unit = deleteDeepPartition(spark,this, partitions)

  def debug(spark: SparkSession): Unit = {
    //TODO loogger??
//    if (SparkAppConfig.instance.isDebug) {
//      println("**************")
//      println(s"* DEBUG: ${this.name}")
//      println("**************")
//      readTable(spark, this, changeSchema = false).show
//    }
  }

  def copy(): DataTable = this.clone().asInstanceOf[DataTable]

//  override def clazz: Class[_] = this.getClass

}

object DataTable {
  private val NAME = "name"
  private val PATH = "path"
  private val INPUT_PATH = "inputPath"
  private val OUTPUT_PATH = "outputPath"
  private val FORMAT = "format"
  private val READ_ONLY = "readOnly"
  private val MODE = "mode"
  private val PKS = "pks"
  private val PARTITION_COLUMNS = "partitionColumns"
  private val INCLUDE_PARTITIONS_AS_PK = "includePartitionsAsPk"
  private val SORT_PKS = "sortPks"
  private val PRESERVE_SCHEMA = "preserveSchema"
  private val PROPERTIES = "properties"

  def apply(config: Map[String, Any]): DataTable = {
    val (inputPath, outputPath, updatable) = if (config.contains(INPUT_PATH)) {
      if (!config.contains(OUTPUT_PATH)) {
        throw new MandatoryKeyNotFound(MandatoryKeyNotFoundErrors.ioPathKeyNotFoundError.code,
          MandatoryKeyNotFoundErrors.ioPathKeyNotFoundError.message)
      }
      val iPath = config(INPUT_PATH).toString
      val oPath = config(OUTPUT_PATH).toString
      (iPath, oPath, iPath.equals(oPath))
    } else {
      (config(PATH).toString, config(PATH).toString, true)
    }
    val partitionsColumns = Utils.toSeq[String](config(PARTITION_COLUMNS))
    val includePartitions = config.getOrElse(INCLUDE_PARTITIONS_AS_PK, false)
    val sortPks: Any = config.getOrElse(SORT_PKS, false)
    val readOnly = config.getOrElse(READ_ONLY, false).toString.toBoolean
    val preserveSchema = config.getOrElse(PRESERVE_SCHEMA, false).toString.toBoolean

    val pks: Seq[String] = (includePartitions, sortPks) match {
      case (false, false) => Utils.toSeq[String](config(PKS))
      case (false, true) => Utils.toSeq[String](config(PKS)).sortBy(pk => pk)
      case (true, false) => Utils.toSeq[String](config(PKS)) ++ partitionsColumns
      case (true, true) => (Utils.toSeq[String](config(PKS)) ++ partitionsColumns).sortBy(pk => pk)
    }

    new DataTable(config(NAME).toString,
      inputPath, outputPath,
      updatable,
      readOnly,
      config(FORMAT).toString,
      config(MODE).toString,
      pks,
      partitionsColumns.nonEmpty,
      partitionsColumns,
      Some(Utils.toMap[String](config(PROPERTIES))),
      preserveSchema)

  }
}