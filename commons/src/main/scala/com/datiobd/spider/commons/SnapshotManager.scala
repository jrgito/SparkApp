package com.datiobd.spider.commons

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar

class Snapshot() {

}

object SnapshotManager {
  val c: Calendar = newCalendar

  private def newCalendar: Calendar = {
    val c = Calendar.getInstance
    c.set(Calendar.HOUR, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c
  }

  object Type extends Enumeration {
    val DAILY, MONTHLY = Value
  }

  object Month extends Enumeration {
    val ACTUAL =Value(-1)
    val NEWEST =ACTUAL
    val PREVIOUS = Value(-2)
    val NEXT_TO_LAST =Value(-59)
    val OLDEST =Value(-60)
  }

  object Day extends Enumeration {
    val ACTUAL =Value(-1)
    val NEWEST =ACTUAL
    val PREVIOUS = Value(-2)
    val NEXT_TO_LAST =Value(-3)
    val OLDEST =Value(-4)
  }

  val monthSdf = new SimpleDateFormat("yyyy-MM")

  def getSnapshot(which: SnapshotManager.Month.Value): Date = {
    val _c = c.clone().asInstanceOf[Calendar]
    _c.add(Calendar.MONTH, which.id)
    _c.set(Calendar.DATE, _c.getActualMaximum(Calendar.DAY_OF_MONTH))
    new Date(_c.getTimeInMillis)
  }

  def getSnapshot(which: SnapshotManager.Day.Value)(implicit d: DummyImplicit): Date = {
    val _c = c.clone().asInstanceOf[Calendar]
    _c.add(Calendar.DATE, which.id)
    new Date(_c.getTimeInMillis)
  }

  def getSnapshot(which: SnapshotManager.Type.Value, calendar: Calendar): Date = {
    val _c = calendar.clone().asInstanceOf[Calendar]
    which match {
      case SnapshotManager.Type.MONTHLY => _c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
      case SnapshotManager.Type.DAILY => _c.set(Calendar.DATE, c.get(Calendar.DATE))
    }
    new Date(_c.getTimeInMillis)
  }
}
