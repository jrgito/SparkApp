package com.datiobd.spider.commons

import org.apache.spark.sql.types.StructType

import scala.collection.JavaConversions._

object Utils {
  /**
    * transform any object in seq[T]
    * @param o {Any} scala.collection.Seq[T] | java.util.ArrayList[T]
    * @tparam T {T} String
    * @return Seq[T]
    */
  def toSeq[T](o: Any): Seq[T] = {
    o match {
      case a: java.util.ArrayList[T] => o.asInstanceOf[java.util.ArrayList[T]].toSeq
      case b: scala.collection.Seq[T] => o.asInstanceOf[Seq[T]]
    }
  }

  /**
    * transform any object in seq[T,T]
    * @param o {Any} java.util.Map[T, T]
    * @tparam T {T} String
    * @return Map[T,T]
    */
  def toMap[T](o: Any): Map[T, T] = {
    o match {
      case a: java.util.Map[T, T] => o.asInstanceOf[java.util.Map[T, T]].toMap
      case _ => o.asInstanceOf[Map[T, T]]
    }
  }


  /**
    * check if two struct type are equals (length, names, types)
    *
    * @param schema1 {StructType}
    * @param schema2 {StructType}
    */
  def areEqual(schema1: StructType, schema2: StructType): Unit = {
    System.err.println("**********\n* Comparing df...\n**********")
    hasDifferentNumberColumns(schema1, schema2)
    hasDifferentColumnsName(schema1, schema2)
    hasDifferentDataTypes(schema1, schema2)
  }

  /**
    * check if two struct type are equals (length)
    *
    * @param schema1 {StructType}
    * @param schema2 {StructType}
    * @return false or Exception
    */

  private def hasDifferentNumberColumns(schema1: StructType, schema2: StructType) = {
    if (!(schema2.fields.length == schema1.length)) {
      throw new Exception(s"struct is not equal")
    }
  }


  /**
    * check if two struct type are equals (names)
    *
    * @param schema1 {StructType}
    * @param schema2 {StructType}
    * @return false or Exception
    */
  private def hasDifferentColumnsName(schema1: StructType, schema2: StructType) {
    schema1.fields.foreach(fieldSchema =>
      if (!schema2.map(_.name).contains(fieldSchema.name)) {
        throw new Exception(s"field ${fieldSchema.name} not found")
      })
  }

  /**
    * check if two struct type are equals (types)
    *
    * @param schema1 {StructType}
    * @param schema2 {StructType}
    * @return false or Exception
    */
  private def hasDifferentDataTypes(schema1: StructType, schema2: StructType) {
    schema2.fields.foreach(fieldSchema =>
      if (!(schema1.find(_.name == fieldSchema.name).head.dataType == fieldSchema.dataType)) {
        throw new Exception(s"field ${fieldSchema.name} has different type")
      })
  }

}
