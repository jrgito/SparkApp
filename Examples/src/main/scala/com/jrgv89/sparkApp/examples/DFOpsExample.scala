package com.jrgv89.sparkApp.examples

import com.jrgv89.sparkApp.ops.dfOps.DFOps
import org.apache.spark.sql.SparkSession

object DFOpsExample extends App with DFOps {

  //override this method to configurate hdfs. With sparkApp this does not make sense
  override def isHDFSEnable: Boolean = false

  //  override def getHDFSConfig: Configuration = super.getHDFSConfig

  val spark = SparkSession.builder().master("local[2]").getOrCreate()

  import spark.implicits._

  val rows = Seq(("aa", "bb", 10), ("cc", "dd", 20))

  val df = rows.toDF("name", "surname", "age")

  // write df

  writeDF(df, "out/parquetDF", "parquet", "overwrite")

  // read df

  val df2 = readDF(spark, "out/parquetDF", "parquet")
  df2.show

  //write partitioned DF

  writeDF(df2, "out/partitioned", "parquet", "overwrite", Seq("age"))

  readDF(spark, "out/partitioned", "parquet").show

  //read one partition

  val df3 = readDF(spark, "out/partitioned", "parquet", partitions = Some(Seq(("age", 10))))

  df3.show

  //update dataframe

  val df4 = Seq(("aa", "bb", 30)).toDF("name", "surname", "age")

  updateDF(df3.union(df4), "out/partitioned", "parquet", None, Seq("age"))

  readDF(spark, "out/partitioned", "parquet").show
}
