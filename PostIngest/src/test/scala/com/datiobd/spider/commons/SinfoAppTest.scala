package com.datiobd.spider.commons

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSpec

class SinfoAppTest extends FunSpec {

  class _SinfoAppMock(configFile: String) extends SinfoApp(configFile) {
    override def execute(): Unit = {}
  }

  describe("with sinfoApp1") {
    val sa = new _SinfoAppMock(new File("src/test/resources/sinfoApp1.conf").getCanonicalPath)
    it("parseConfigTables") {
      assert(sa.tables.size == 2)
      assert(sa.tables.toSeq.head._1.equals("demo"))
      assert(sa.tables.toSeq.last._1.equals("demo2"))
      assert(sa.tables("demo2").pks.isEmpty)
      assert(sa.tables.forall(t=>t._1.equals(t._2.name)))
      assert(sa.tables.isInstanceOf[Map[String, Table]])
    }
    it("isDebug"){
      assert(!sa.isDebug)
    }
    describe("parseHDFSConfig") {
      it("isHDFSEnable") {
        assert(!sa.isHDFSEnable)
      }
      it("getHDFSConfig") {
        assert(sa.getHDFSConfig.get("fs.defaultFS").equals(new Configuration().get("fs.defaultFS")))
      }
    }
    sa.sc.stop()
  }

  describe("with sinfoApp2") {
    val sa = new _SinfoAppMock(new File("src/test/resources/sinfoApp2.conf").getCanonicalPath)
    it("parseConfigTables") {
      assert(sa.tables.isEmpty)
    }
    it("isDebug"){
      assert(!sa.isDebug)
    }
    describe("parseHDFSConfig") {
      it("isHDFSEnable") {
        assert(!sa.isHDFSEnable)
      }
      it("getHDFSConfig") {
        assert(sa.getHDFSConfig.get("fs.defaultFS").equals(new Configuration().get("fs.defaultFS")))
      }
    }
    sa.sc.stop()
  }

  describe("with sinfoApp3") {
    val sa = new _SinfoAppMock(new File("src/test/resources/sinfoApp3.conf").getCanonicalPath)
    it("parseConfigTables") {
      assert(sa.tables.size == 1)
      assert(sa.tables.toSeq.head._1.equals("demo"))
      assert(sa.tables("demo").pks.length == 3)
      val list = Seq("field1", "field2", "field3")
      assert(sa.tables.head._2.pks.zip(list).forall(n=>n._1.equals(n._2)))
      assert(sa.tables.forall(t=>t._1.equals(t._2.name)))
      assert(sa.tables.isInstanceOf[Map[String, Table]])
    }
    it("isDebug"){
      assert(sa.isDebug)
    }
    describe("parseHDFSConfig") {
      it("isHDFSEnable") {
        assert(sa.isHDFSEnable)
      }
      it("getHDFSConfig") {
        assert(!sa.getHDFSConfig.get("fs.defaultFS").equals(new Configuration().get("fs.defaultFS")))
        assert(sa.getHDFSConfig.isInstanceOf[Configuration])
      }
    }
    sa.sc.stop()
  }


}
