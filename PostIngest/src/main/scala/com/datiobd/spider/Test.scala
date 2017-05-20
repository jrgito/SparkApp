package com.datiobd.spider

import com.datiobd.spider.commons.{SparkApp, SparkAppConfig}
import com.datiobd.spider.commons.table.TableBuilder

/**
  * Created by JRGv89 on 20/05/2017.
  */
class Test(path:String) extends SparkApp(path){
  override def execute(): Unit = println("test")
}

object Test extends App {
  new Test("PostIngest/src/main/resources/horizontalization/horizontalization.conf")
}
