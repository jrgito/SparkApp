package com.datiobd.spider.commons

import java.text.SimpleDateFormat
import java.util.Calendar

import org.scalatest.FunSpec

class TimeUtilsTest extends FunSpec {
  val timeUtils = new TimeUtils {}
  val sdfDate = new SimpleDateFormat("yyyy-MM-dd")
  val sdfTime = new SimpleDateFormat("yyyy-MM-dd mm:ss")

  describe("TimeUtilsTest") {

    describe("newInitCalendar") {
      it("when parameter is START") {
        val test = timeUtils.newInitCalendar(timeUtils.START)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.DATE, 1)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is END") {
        val test = timeUtils.newInitCalendar(timeUtils.END)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is TODAY") {
        val test = timeUtils.newInitCalendar(timeUtils.TODAY)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is other") {
        val test = timeUtils.newInitCalendar(8)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
    }

    describe("addMonthAndInit") {
      describe("when ADD months and init to START") {
        val _c = Calendar.getInstance
        _c.set(Calendar.HOUR, 0)
        _c.set(Calendar.MINUTE, 0)
        _c.set(Calendar.SECOND, 0)
        _c.set(Calendar.DATE, 1)
        it("when modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.START)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          c.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))

        }
        it("when not modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.START, modify = false)
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          c1.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        }
      }
      describe("when ADD months and init to END") {
        val _c = Calendar.getInstance
        _c.set(Calendar.HOUR, 0)
        _c.set(Calendar.MINUTE, 0)
        _c.set(Calendar.SECOND, 0)
        _c.set(Calendar.DATE, _c.getActualMaximum(Calendar.DAY_OF_MONTH))
        it("when modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.END)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          c.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))

        }
        it("when not modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.END, modify = false)
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          c1.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        }
      }
      describe("when ADD months and init to TODAY") {
        val _c = Calendar.getInstance
        _c.set(Calendar.HOUR, 0)
        _c.set(Calendar.MINUTE, 0)
        _c.set(Calendar.SECOND, 0)
        it("when modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.TODAY)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          c.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }
        it("when not modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, timeUtils.TODAY, modify = false)
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          c1.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        }
      }
      describe("when ADD months and init to other") {
        val _c = Calendar.getInstance
        _c.set(Calendar.HOUR, 0)
        _c.set(Calendar.MINUTE, 0)
        _c.set(Calendar.SECOND, 0)
        it("when modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, 4)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          c.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }
        it("when not modify calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          val c1 = _c.clone().asInstanceOf[Calendar]
          val test = timeUtils.addMonthAndInit(c1, 2, 4, modify = false)
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          c1.add(Calendar.MONTH, 2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        }
      }
    }

    describe("newInitDate") {
      it("when parameter is START") {
        val test = timeUtils.newInitDate(timeUtils.START)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.DATE, 1)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is END") {
        val test = timeUtils.newInitDate(timeUtils.END)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is TODAY") {
        val test = timeUtils.newInitDate(timeUtils.TODAY)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
      it("when parameter is other") {
        val test = timeUtils.newInitDate(8)
        val c = Calendar.getInstance
        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
      }
    }

    describe("previousMonth") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      describe("when modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.START)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.END)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.TODAY)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, 20)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }

      describe("when not modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.START, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.END, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, timeUtils.TODAY, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonth(c2, 20, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }
    }

    describe("previousMonthDate") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      describe("when modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.START)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.END)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.TODAY)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, 20)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }

      describe("when not modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.START, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.END, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, timeUtils.TODAY, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.MONTH, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousMonthDate(c2, 20, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }
    }

    describe("initPreviousMonth") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      it("when parameter is START") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        c1.set(Calendar.DATE, 1)
        val test = timeUtils.initPreviousMonth(timeUtils.START)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is END") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
        val test = timeUtils.initPreviousMonth(timeUtils.END)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is TODAY") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        val test = timeUtils.initPreviousMonth(timeUtils.TODAY)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is other") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        val test = timeUtils.initPreviousMonth(20)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
    }

    describe("initPreviousMonthDate") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      it("when parameter is START") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        c1.set(Calendar.DATE, 1)
        val test = timeUtils.initPreviousMonthDate(timeUtils.START)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is END") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
        val test = timeUtils.initPreviousMonthDate(timeUtils.END)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is TODAY") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        val test = timeUtils.initPreviousMonthDate(timeUtils.TODAY)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is other") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.MONTH, -1)
        val test = timeUtils.initPreviousMonthDate(20)
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
    }

    describe("updateMonth") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      describe("when modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.START)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.END)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.TODAY)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is Other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, 9)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }

      describe("when not modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          c1.set(Calendar.DATE, 1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.START, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          c1.set(Calendar.DATE, c1.getActualMaximum(Calendar.DAY_OF_MONTH))
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.END, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, timeUtils.TODAY, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is Other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.set(Calendar.MONTH, 11)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.updateMonth(c2, 11, 9, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }
    }

    describe("previousDay") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      describe("when modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }

      describe("when not modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDay(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }
    }

    describe("previousDayDate") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      describe("when modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }

      describe("when not modify calendar") {
        it("when parameter is START") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is END") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is TODAY") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
        it("when parameter is other") {
          val c1 = c.clone().asInstanceOf[Calendar]
          c1.add(Calendar.DATE, -1)
          val c2 = c.clone().asInstanceOf[Calendar]
          val test = timeUtils.previousDayDate(c2, modify = false)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
          assert(!sdfDate.format(test.getTime).equals(sdfDate.format(c2.getTime)))
          assert(!sdfTime.format(test.getTime).equals(sdfTime.format(c2.getTime)))
        }
      }
    }

    describe("initPreviousDay") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      it("when parameter is START") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDay()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is END") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDay()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is TODAY") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDay()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is other") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDay()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
    }

    describe("initPreviousDayDate") {
      val c = Calendar.getInstance
      c.set(Calendar.HOUR, 0)
      c.set(Calendar.MINUTE, 0)
      c.set(Calendar.SECOND, 0)
      it("when parameter is START") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDayDate()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is END") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDayDate()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is TODAY") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDayDate()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
      it("when parameter is other") {
        val c1 = c.clone().asInstanceOf[Calendar]
        c1.add(Calendar.DATE, -1)
        val test = timeUtils.initPreviousDayDate()
        assert(sdfDate.format(test.getTime).equals(sdfDate.format(c1.getTime)))
        assert(sdfTime.format(test.getTime).equals(sdfTime.format(c1.getTime)))
        assert(!sdfDate.format(c.getTime).equals(sdfDate.format(c1.getTime)))
        assert(!sdfTime.format(c.getTime).equals(sdfTime.format(c1.getTime)))
      }
    }

    describe("calendarToSQLString") {
      it("passing a calendar") {

        val c = Calendar.getInstance
        c.setTime(sdfDate.parse("2017-01-01"))
        assert(timeUtils.calendarToSQLString(c).equals("'2017-01-01'"))
      }
    }

    describe("test for close dates") {
      val _c = Calendar.getInstance
      _c.set(Calendar.HOUR, 0)
      _c.set(Calendar.MINUTE, 0)
      _c.set(Calendar.SECOND, 0)
      describe("int") {
        it("getCloseDateOf(int) should add int months, init to last day of the month and return calendar") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.add(Calendar.MONTH, -1)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          val test = timeUtils.getCloseCalendarOf(-1)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }

        it("getCloseDateOf(int) should add int months, init to last day of the month and return date") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.add(Calendar.MONTH, -1)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          val test = timeUtils.getCloseDateOf(-1)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }

        it("getCloseDateOf(int) should add int months, init to last day of the month and return timestamp") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.add(Calendar.MONTH, -1)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          val test = timeUtils.getCloseTimestampOf(-1)
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }
      }
      describe("calendar") {
        //Calendar
        it("getCloseDateOf(calendar) should return the close date of the month of the calendar in calendar format") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.set(Calendar.YEAR, 2017)
          c.set(Calendar.MONTH, 11)
          val test = timeUtils.getCloseCalendarOf(c)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }

        it("getCloseDateOf(calendar) should return the close date of the month of the calendar in date format") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.set(Calendar.YEAR, 2017)
          c.set(Calendar.MONTH, 11)
          val test = timeUtils.getCloseCalendarOf(c)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }

        it("getCloseDateOf(calendar) should return the close date of the month of the calendar in timestamp format") {
          val c = _c.clone().asInstanceOf[Calendar]
          c.set(Calendar.YEAR, 2017)
          c.set(Calendar.MONTH, 11)
          val test = timeUtils.getCloseCalendarOf(c)
          c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
          assert(sdfDate.format(test.getTime).equals(sdfDate.format(c.getTime)))
          assert(sdfTime.format(test.getTime).equals(sdfTime.format(c.getTime)))
        }
      }
    }
  }
}


