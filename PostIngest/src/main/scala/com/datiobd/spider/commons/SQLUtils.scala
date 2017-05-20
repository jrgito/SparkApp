package com.datiobd.spider.commons

import com.datiobd.spider.commons.table.Table

trait SQLUtils {
  private val AND = " and "
  private val SEPARATOR = ", "
  private val GROUP_BY = "GROUP BY "

  @deprecated("use createJoinClause(df1: String, df2: String, columns: Seq[String])")
  def createJoinClause(columns: Seq[String], df1: String, df2: String): String = {
    columns.map(c => s"$df1.$c = $df2.$c").mkString(AND)
  }

  /**
    * create a join clause from two dataFrames and their columns
    *
    * @param df1     {String} name of first DataFrame
    * @param df2     {String} name of second DataFrame
    * @param columns {Seq[String]} columns for creating join
    * @return {String} join clause
    */
  def createJoinClause(df1: String, df2: String, columns: Seq[String]): String = {
    columns.map(c => s"$df1.$c = $df2.$c").mkString(AND)
  }
  /**
    * Create a join clause from two dataFrames and their columns.
    * Join clause will have the same columns
    *
    * @param df1      {String} name of first DataFrame
    * @param columns1 {Seq[String]} columns for first DataFrame
    * @param df2      {String} name of second DataFrame
    * @param columns2 {Seq[String]} columns for second DataFrame
    * @return {String} join clause
    */
  def createJoinClause(df1: String, columns1: Seq[String], df2: String, columns2: Seq[String]): String = {
    columns1.map(c => if (columns2.contains(c)) s"$df1.$c = $df2.$c" else "").filterNot(_.isEmpty).mkString(AND)
  }

  /**
    * Create a join clause between a table and a dataframe.
    * Join clause will have the same columns
    *
    * @param table   {Table} table
    * @param df      {String} name of dataframe
    * @param columns {Seq[String]}
    * @return {String} join clause
    */
  def createJoinClause(table: Table, df: String, columns: Seq[String]): String = {
    table.pks.map(c => if (columns.contains(c)) s"${table.name}.$c = $df.$c" else "").filterNot(_.isEmpty).mkString(AND)
  }
  /**
    * Create a join clause between two tables.
    * Join clause will have the same columns
    *
    * @param table1 {Table} table1
    * @param table2 {Table} table2
    * @return {String} join clause
    */
  def createJoinClause(table1: Table, table2: Table): String = {
    table1.pks.map(c => if (table2.pks.contains(c)) s"${table1.name}.$c = ${table2.name}.$c" else "").filterNot(_.isEmpty).mkString(AND)
  }

  /**
    * create a join clause from two dataFrames and their zipped columns
    *
    * @param df1     {String} name of first DataFrame
    * @param df2     {String} name of second DataFrame
    * @param zipColumns {Seq[(String,String)]} columns for creating join
    * @return {String} join clause
    */
  def createZipJoinClause(df1: String, df2: String, zipColumns: Seq[(String, String)]): String = {
    zipColumns.map(c => s"$df1.${c._1} = $df2.${c._2}").mkString(AND)
  }
  /**
    * create a join clause from table and dataframe and their zipped columns
    *
    * @param t     {Table} name of first DataFrame
    * @param df     {String} name of second DataFrame
    * @param zipColumns {Seq[(String,String)]} columns for creating join
    * @return {String} join clause
    */
  def createZipJoinClause(t: Table, df: String, zipColumns: Seq[(String, String)]): String = {
    zipColumns.map(c => s"${t.name}.${c._1} = $df.${c._2}").mkString(AND)
  }
  /**
    * create a join clause from two tables and their zipped columns
    *
    * @param t1     {String} name of first DataFrame
    * @param t2     {String} name of second DataFrame
    * @param zipColumns {Seq[(String,String)]} columns for creating join
    * @return {String} join clause
    */
  def createZipJoinClause(t1: Table, t2: Table, zipColumns: Seq[(String, String)]): String = {
    zipColumns.map(c => s"${t1.name}.${c._1} = ${t2.name}.${c._2}").mkString(AND)
  }

  /**
    * Create a join clause from two dataFrames and their columns.
    * Join clause will be in order of columns
    *
    * @param df1      {String} name of first DataFrame
    * @param columns1 {Seq[String]} columns for first DataFrame
    * @param df2      {String} name of second DataFrame
    * @param columns2 {Seq[String]} columns for second DataFrame
    * @return {String} join clause
    */
  def createJoinClauseByOrder(df1: String, columns1: Seq[String], df2: String, columns2: Seq[String]): String = {
    columns1.zip(columns2).map(c => s"$df1.${c._1} = $df2.${c._2}").mkString(AND)
  }

  /**
    * Create a join clause between table and df.
    * Join clause will be between pks and columns in order
    *
    * @param table   {Table} table
    * @param df      {String} name of first DataFrame
    * @param columns {Seq[String]} columns for first DataFrame
    * @return {String} join clause
    */
  def createJoinClauseByOrder(table: Table, df: String, columns: Seq[String]): String = {
    table.pks.zip(columns).map(c => s"${table.name}.${c._1} = $df.${c._2}").mkString(AND)
  }

  /**
    * Create a join clause between two tables.
    * Join clause will be between pks in order
    *
    * @param table1   {Table} first table
    * @param columns1 {Seq[String]} columns for first DataFrame
    * @param table2   {Table} second table
    * @param columns2 {Seq[String]} columns for second DataFrame
    * @return {String} join clause
    */
  def createJoinClauseByOrder(table1: Table, columns1: Seq[String], table2: Table, columns2: Seq[String]): String = {
    columns1.zip(columns2).map(c => s"${table1.name}.${c._1} = ${table2.name}.${c._2}").mkString(AND)
  }

  /**
    * Create a join clause between table and df.
    * Join clause will be between pks in order
    *
    * @param table1 {Table} table1
    * @param table2 {Table} table2
    * @return {String} join clause
    */
  def createJoinClauseByOrder(table1: Table, table2: Table): String = {
    table1.pks.zip(table2.pks).map(c => s"${table1.name}.${c._1} = ${table2.name}.${c._2}").filterNot(_.isEmpty).mkString(AND)
  }

  /**
    * create a group by clause according to columns
    *
    * @param columns {Seq[String]} columns for join
    * @return {String} group by clause
    */
  def createGroupByClause(columns: Seq[String]): String = {
    GROUP_BY + columns.mkString(",")
  }

  /**
    * create a group by clause according to df and its columns
    *
    * @param df      {String} df name
    * @param columns {Seq[String]} columns to do a group by
    * @return {String} group by clause
    */
  def createGroupByClause(df: String, columns: Seq[String]): String = {
    GROUP_BY + columns.map(c => s"$df.$c").mkString(SEPARATOR)
  }

  /**
    * create a group by clause according to table and the columns
    *
    * @param table   {Table} table
    * @param columns {Seq[String]} columns to do a group by
    * @return {String} group by clause
    */
  def createGroupByClause(table: Table, columns: Seq[String]): String = {
    GROUP_BY + columns.map(c => s"${table.name}.$c").mkString(SEPARATOR)
  }

  /**
    * create a group by clause according to table and its pks
    *
    * @param table {Table} table
    * @return {String} group by clause
    */
  def createGroupByClause(table: Table): String = {
    GROUP_BY + table.pks.map(c => s"${table.name}.$c").mkString(SEPARATOR)
  }

  /**
    * create a select clause according to a dataframe with it s columns
    *
    * @param df      {String} dataframe
    * @param columns {Seq[String]} columns to select
    * @return {String} select clause
    */
  def createSelectClause(df: String, columns: Seq[String]): String = {
    columns.map(c => s"$df.$c").mkString(SEPARATOR)
  }

  /**
    * create a select clause according to table and its pks
    *
    * @param table {Table} table
    * @return {String} select clause
    */
  def createSelectClause(table: Table): String = {
    table.pks.map(c => s"${table.name}.$c").mkString(SEPARATOR)
  }

  /**
    * create a select clause with a operation for each column util for group By by Clause
 *
    * @param operation      {String} operation to apply in column
    * @param columns        {Seq[String]} columns to select
    * @return
    */
  def createOperatedSelectClause(operation:String, columns: Seq[String]): String = {
    columns.map(c => s"$operation($c)").mkString(SEPARATOR)
  }
  /**
    * create a select clause with a operation for each column util for group By by Clause
 *
    * @param df             {String} dataframe
    * @param operation      {String} operation to apply in column
    * @param columns        {Seq[String]} columns to select
    * @return
    */
  def createOperatedSelectClause(df: String, operation:String, columns: Seq[String]): String = {
    columns.map(c => s"$operation($df.$c)").mkString(SEPARATOR)
  }

  /**
    * create a select clause with a operation for each column util for group By by Clause
 *
    * @param table          {Table} table
    * @param operation      {String} operation to apply in column
    * @return
    */
  def createOperatedSelectClause(table: Table, operation:String): String = {
    table.pks.map(c => s"$operation(${table.name}.$c)").mkString(SEPARATOR)
  }



}
