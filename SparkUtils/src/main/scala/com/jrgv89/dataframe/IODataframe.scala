package com.jrgv89.dataframe

import com.jrgv89.dataframe.dfOps.DFOps
import org.apache.spark.sql.DataFrame

object IODataframe {

  implicit class IODataFrame(dataFrame: DataFrame) extends DFOps{

  }

}


object A {

  var df :DataFrame = null
  df.wr
}