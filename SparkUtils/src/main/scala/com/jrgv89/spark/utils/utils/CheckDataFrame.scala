package com.jrgv89.spark.utils.utils

import com.jrgv89.spark.utils.datatable.exceptions.{SchemaErrors, SchemaException}
import org.apache.spark.sql.types.StructType

/**
  * Created by JRGv89 on 20/05/2017.
  */
object CheckDataFrame {


  /**
    * check if two struct type are equals (length, names, types)
    *
    * @param leftSchema  {StructType}
    * @param rightSchema {StructType}
    */
  def areEquals(leftSchema: StructType, rightSchema: StructType): Boolean = {
    !hasDifferentNumberColumns(leftSchema, rightSchema) &&
      !hasDifferentColumnsName(leftSchema, rightSchema) &&
      !hasDifferentDataTypes(leftSchema, rightSchema)
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
      throw new SchemaException(SchemaErrors.columnsNumberError.code,
        SchemaErrors.columnsNumberError.message.format(leftSchema.length, rightSchema.length))
    }
    false
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
        throw new SchemaException(SchemaErrors.columnsNameError.code,
          SchemaErrors.columnsNameError.message.format(fieldSchema.name, rightSchema.map(_.name)))
      })
    false
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
        throw new SchemaException(SchemaErrors.columnsTypesError.code,
          SchemaErrors.columnsTypesError.message.format(fieldSchema.name, fieldSchema.dataType, rightSchema.find(_.name == fieldSchema.name).head.dataType))
      })
    false
  }
}
