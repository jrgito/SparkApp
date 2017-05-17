package com.datiobd.spider.commons

import org.scalatest.FunSpec

/**
  * Created by jmagallon on 17/01/17.
  */
class SQLUtilsTest extends FunSpec {
  val mockSQL = new SQLUtils {}
  val df1 = "df1"
  val df2 = "df2"
  val s1 = Seq("c1", "c2", "c3")
  val s2 = Seq("c1", "c3")
  val s3 = Seq("c1", "c3", "c2")
  val t1 = Table("table1", "", "", "", s1, partitions = false, Seq(), None)
  val t2 = Table("table2", "", "", "", s2, partitions = false, Seq(), None)
  val t3 = Table("table3", "", "", "", s3, partitions = false, Seq(), None)

  describe("SQLUtilsTest") {
    describe("createJoinClause") {

      it("df1: String, df2: String, columns: Seq[String]") {
        val test = "df1.c1 = df2.c1 and df1.c2 = df2.c2 and df1.c3 = df2.c3"
        assert(mockSQL.createJoinClause(df1, df2, s1).equals(test))
      }
      it("df1: String, columns1: Seq[String], df2: String, columns2: Seq[String]") {
        val test = "df1.c1 = df2.c1 and df1.c3 = df2.c3"
        assert(mockSQL.createJoinClause(df1, s1, df2, s2).equals(test))
      }
      it("table1: Table, table2: Table") {
        val test = "table1.c1 = table2.c1 and table1.c3 = table2.c3"
        assert(mockSQL.createJoinClause(t1, t2).equals(test))
      }
      it("table1: Table, df: String, columns: Seq[String]") {
        val test = "table1.c1 = df1.c1 and table1.c2 = df1.c2 and table1.c3 = df1.c3"
        assert(mockSQL.createJoinClause(t1, df1, s3).equals(test))
      }
    }
    describe("createJoinClauseByOrder") {

      it("df1: String, columns1: Seq[String], df2: String, columns2: Seq[String]") {
        val test = "df1.c1 = df2.c1 and df1.c2 = df2.c3"
        assert(mockSQL.createJoinClauseByOrder(df1, s1, df2, s2).equals(test))
      }
      it("table: Table, df: String, columns: Seq[String]") {
        val test = "table1.c1 = df1.c1 and table1.c2 = df1.c2 and table1.c3 = df1.c3"
        assert(mockSQL.createJoinClauseByOrder(t1, df1, s1).equals(test))
      }
      it("table1: Table, columns1: Seq[String], table2: Table, columns2: Seq[String]") {
        val test = "table1.c1 = table2.c1 and table1.c3 = table2.c3"
        assert(mockSQL.createJoinClauseByOrder(t1, s2, t2, s3).equals(test))
      }
      it("table1: Table, table2: Table") {
        val test = "table1.c1 = table2.c1 and table1.c2 = table2.c3"
        assert(mockSQL.createJoinClauseByOrder(t1, t2).equals(test))
      }
    }

    describe("createGroupByClause") {
      it("df: String, columns: Seq[String]") {
        val test = "GROUP BY df1.c1, df1.c2, df1.c3"
        assert(mockSQL.createGroupByClause(df1, s1).equals(test))
      }
      it("table: Table, columns: Seq[String]") {
        val test = "GROUP BY table1.c1, table1.c3"
        assert(mockSQL.createGroupByClause(t1, s2).equals(test))
      }
      it("table: Table") {
        val test = "GROUP BY table1.c1, table1.c2, table1.c3"
        assert(mockSQL.createGroupByClause(t1).equals(test))
      }
    }
  }
}
