package com.datiobd.spider.commons.utils

import com.datiobd.spider.commons.exceptions.SchemaException
import org.apache.spark.sql.types.StructType

/**
  * Created by JRGv89 on 20/05/2017.
  */
object CheckDataFrame {

  private val columnsNumberError = "Columns numbers are distinct: right (%d) != left (%d)"
  private val columnsNameError = "field %s not found in right schema %s"
  private val columnsTypesError = "field %s has different type left: %s != right: %s"

  /**
    * check if two struct type are equals (length, names, types)
    *
    * @param leftSchema  {StructType}
    * @param rightSchema {StructType}
    */
  def areEqual(leftSchema: StructType, rightSchema: StructType): Unit = {
    System.err.println("**********\n* Comparing df...\n**********")
    hasDifferentNumberColumns(leftSchema, rightSchema)
    hasDifferentColumnsName(leftSchema, rightSchema)
    hasDifferentDataTypes(leftSchema, rightSchema)
  }

  /**
    * check if two struct type are equals (length)
    *
    * @param leftSchema  {StructType}
    * @param rightSchema {StructType}
    * @return false or Exception
    */

  private def hasDifferentNumberColumns(leftSchema: StructType, rightSchema: StructType): Boolean = {
    if (!(leftSchema.length == rightSchema.length)) {
      throw new SchemaException(columnsNumberError.format(leftSchema.length, rightSchema.length))
    }
    true
  }


  /**
    * check if two struct type are equals (names)
    *
    * @param leftSchema  {StructType}
    * @param rightSchema {StructType}
    * @return false or Exception
    */
  private def hasDifferentColumnsName(leftSchema: StructType, rightSchema: StructType): Boolean = {
    leftSchema.fields.foreach(fieldSchema =>
      if (!rightSchema.map(_.name).contains(fieldSchema.name)) {
        throw new SchemaException(columnsNameError.format(fieldSchema.name, rightSchema.map(_.name).toSeq))
      })
    true
  }

  /**
    * check if two struct type are equals (types)
    *
    * @param leftSchema  {StructType}
    * @param rightSchema {StructType}
    * @return false or Exception
    */
  private def hasDifferentDataTypes(leftSchema: StructType, rightSchema: StructType): Boolean = {
    leftSchema.fields.foreach(fieldSchema =>
      if (!(rightSchema.find(_.name == fieldSchema.name).head.dataType == fieldSchema.dataType)) {
        throw new SchemaException(columnsTypesError.format(fieldSchema.name, fieldSchema.dataType, rightSchema.find(_.name == fieldSchema.name).head.dataType))
      })
    true
  }
}
