package com.jrgv89.sparkApp

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class SparkAppTest extends FeatureSpec with GivenWhenThen with Matchers {

  val spark = SparkSession.builder().master("local").getOrCreate()

  import spark.implicits._

  val df = Seq((Map(("p", "2")), 3)).toDF("a", "b")


  val inSchema: StructType = StructType(Seq(StructField("a", StringType), StructField("b", StringType)))
  val schema = StructType(Seq(StructField("key", inSchema), StructField("value", StringType)))

  schema.printTreeString()

  val in = Row("1", "2")
  val seq = Row(in, "3")
  val r = spark.sparkContext.parallelize(Seq(seq))


  val df2 = spark.createDataFrame(r, schema)

  df.show
  df2.printSchema()
  df2.show


  feature("spark app") {

    scenario("Process") {

      Given("Given a environment with data and a dataframe to test")


      When("When execute process")


      Then("Then should get array of ParametricTableRow equals to test")


    }
  }
}
