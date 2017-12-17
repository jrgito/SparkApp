package com.jrgv89.spark.utils.dataframe

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object Implicits {

  implicit class ExtSparkSession(val spark: SparkSession) extends Pop with PopPartition

  implicit class ExtDataFrame(val dataFrame: DataFrame) extends Push with PushPartition {
    /**
      * returns a dataFrame with timestamp
      *
      * @param column {Column}
      * @return dataframe with timestamp column
      */
    def withTS(column: String): DataFrame = dataFrame.withColumn(column, lit(new Timestamp(Calendar.getInstance().getTimeInMillis)))

  }

}
