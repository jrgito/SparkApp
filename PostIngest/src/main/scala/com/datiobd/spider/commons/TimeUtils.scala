package com.datiobd.spider.commons

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

@deprecated("use Snapshot wiip")
trait TimeUtils {
  //todo: modify to enum
  val START: Int = 0
  val TODAY: Int = 1
  val END: Int = 2
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  /**
    * get a calendar instance and initialize its day
    * START first day
    * END last day
    * TODAY or other today day
    *
    * @param init {Int} day to init
    * @return {Calendar} calendar initialized
    */
  def newInitCalendar(init: Int): Calendar = {
    val c = Calendar.getInstance
    c.set(Calendar.HOUR, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.DATE, init match {
      case START => 1
      case END => c.getActualMaximum(Calendar.DAY_OF_MONTH)
      case _ => c.get(Calendar.DATE)
    })
    c
  }

  /**
    * add months to a calendar and initializes its day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param c      {Calendar} calendar to initialize
    * @param months {Int} number of months to add
    * @param init   {Int}day to init
    * @param modify {Boolean} if modify calendar
    * @return {Calendar} Calendar initialized
    */
  def addMonthAndInit(c: Calendar, months: Int, init: Int, modify: Boolean = true): Calendar = {
    val _c = if (modify) c else c.clone().asInstanceOf[Calendar]
    _c.add(Calendar.MONTH, months)
    _c.set(Calendar.DATE, init match {
      case START => 1
      case END => _c.getActualMaximum(Calendar.DAY_OF_MONTH)
      case _ => _c.get(Calendar.DATE)
    })
    _c
  }

  /**
    * get a date instance and initialize it to a init day
    * START first day
    * END last day
    * TODAY or other today day
    *
    * @param init {Int} day to init
    * @return {Date} date initialized
    */
  def newInitDate(init: Int): Date = new Date(newInitCalendar(init).getTimeInMillis)

  /**
    * get a previous month of a calendar passed as argument and init it at init day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param c      {Calendar} calendar
    * @param init   {Int}   day to init
    * @param modify {Boolean} if modify calendar
    * @return {Calendar} calendar modified
    */
  def previousMonth(c: Calendar, init: Int, modify: Boolean = true): Calendar = {
    val _c = if (modify) c else c.clone().asInstanceOf[Calendar]
    _c.add(Calendar.MONTH, -1)
    val day = init match {
      case START => 1
      case END => _c.getActualMaximum(Calendar.DAY_OF_MONTH)
      case _ => _c.get(Calendar.DATE)
    }
    _c.set(Calendar.DATE, day)
    _c
  }

  /**
    * get an actual previous month  and init it at init day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param init   {Int}  day to init
    * @param modify {Boolean} if modify calendar
    * @return {Calendar} date initialized
    */
  def previousMonthDate(c: Calendar, init: Int, modify: Boolean = true): Date = {
    new Date(previousMonth(c, init, modify).getTimeInMillis)
  }

  /**
    * get an actual previous month  and init it at init day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param init {Int}  day to init
    * @return {Calendar} calendar initialized
    */
  def initPreviousMonth(init: Int): Calendar = {
    val _c = Calendar.getInstance
    _c.set(Calendar.HOUR, 0)
    _c.set(Calendar.MINUTE, 0)
    _c.set(Calendar.SECOND, 0)
    previousMonth(_c, init)
  }

  /**
    * get an actual previous month  and init it at init day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param init {Int}  day to init
    * @return {Calendar} date initialized
    */
  def initPreviousMonthDate(init: Int): Date = {
    new Date(initPreviousMonth(init).getTimeInMillis)
  }

  /**
    * returns a calendar set to the previous day from a calendar passed as argument
    * This method modify calendar by default
    *
    * @param c      {Calendar} calendar to calculate the previous day
    * @param modify {Boolean} if modify calendar
    * @return {Calendar} modified calendar
    */
  def previousDay(c: Calendar, modify: Boolean = true): Calendar = {
    val _c = if (modify) c else c.clone().asInstanceOf[Calendar]
    _c.add(Calendar.DATE, -1)
    _c
  }

  /**
    * returns a calendar set to the previous day from a calendar passed as argument
    * This method modify calendar by default
    *
    * @param c      {Calendar} calendar to calculate the previous day
    * @param modify {Boolean} if modify calendar
    * @return {Date} modified date
    */
  def previousDayDate(c: Calendar, modify: Boolean = true): Date = {
    new Date(previousDay(c, modify).getTimeInMillis)
  }

  /**
    * init a actual previous day
    *
    * @return {Calendar} modified calendar
    */
  def initPreviousDay(): Calendar = {
    val _c = Calendar.getInstance
    _c.set(Calendar.HOUR, 0)
    _c.set(Calendar.MINUTE, 0)
    _c.set(Calendar.SECOND, 0)
    previousDay(_c)
  }

  /**
    * init a actual previous day
    *
    * @return {Date} modified Date
    */
  def initPreviousDayDate(): Date = {
    new Date(initPreviousDay().getTimeInMillis)
  }

  /**
    * return a calendar for sql query
    *
    * @param c {Calendar} calendar to stringify
    * @return {String} calendar in sql string
    */
  def calendarToSQLString(c: Calendar): String = s"'${new Date(c.getTimeInMillis)}'"

  /**
    * update month from calendar passed with the month and init it at init day
    * START first day
    * END last day
    * TODAY or other today day
    * This method modify the calendar according to modify. True by default
    *
    * @param c      {Calendar} calendar from update
    * @param month  {Int} number of month
    * @param init   {Int} day to init
    * @param modify {Boolean} if modify calendar
    * @return {Calendar} modified calendar
    */
  def updateMonth(c: Calendar, month: Int, init: Int, modify: Boolean = true): Calendar = {
    val _c = if (modify) c else c.clone().asInstanceOf[Calendar]
    _c.set(Calendar.MONTH, month)
    val day = init match {
      case START => 1
      case END => _c.getActualMaximum(Calendar.DAY_OF_MONTH)
      case _ => _c.get(Calendar.DATE)
    }
    _c.set(Calendar.DATE, day)
    _c
  }

  /**
    * method that measure time in execute f
    *
    * @param s {String} text to show before time
    * @param f {T} method to measure
    * @tparam T {T} generic
    * @return {Double} time lapsed in execute f
    */
  def time[T](s: String, f: => T): Double = {
    val start = System.nanoTime
    f
    //TODO review this comment
    println(s"$s time: " + ((System.nanoTime - start) * 1e-9) + " s")
    (System.nanoTime - start) * 1e-9
  }

  //todo: modify to enum
  val ACTUAL = -1
  //  val NEXT = 1
  val PREVIOUS = -2

  val NEWEST_MONTHLY = -1
  val OLDEST_MONTHLY = -60
  val NEWEST_DAILY = -1
  val OLDEST_DAILY = -4
  val monthSdf = new SimpleDateFormat("yyyy-MM")

  /**
    * return a calendar instance at 00:00:00
    *
    * @return {Calendar} calendar initialized
    */
  private def newCalendar: Calendar = {
    val c = Calendar.getInstance
    c.set(Calendar.HOUR, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.SECOND, 0)
    c
  }

  /**
    * returns a calendar initialized at close calendar of add/dim months from today
    *
    * @param addMonth {Int} number of months
    * @return {Calendar} modified calendar
    */
  def getCloseCalendarOf(addMonth: Int): Calendar = {
    val c = newCalendar
    c.add(Calendar.MONTH, addMonth)
    c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
    c
  }

  /**
    * returns a calendar initialized at close Date of add/dim months from today
    *
    * @param addMonth {Int} number of months
    * @return {Date} modified Date
    */
  def getCloseDateOf(addMonth: Int): Date = {
    new Date(getCloseCalendarOf(addMonth).getTimeInMillis)
  }

  /**
    * returns a calendar initialized at close timestamp of add/dim months from today
    *
    * @param addMonth {Int} number of months
    * @return {Timestamp} modified Timestamp
    */
  def getCloseTimestampOf(addMonth: Int): Timestamp = {
    new Timestamp(getCloseCalendarOf(addMonth).getTimeInMillis)
  }

  /**
    * returns a calendar initialized at close date from calendar passed
    *
    * @param c {Calendar} calendar
    * @return {Calendar} modified calendar
    */
  def getCloseCalendarOf(c: Calendar): Calendar = {
    val _c = c.clone().asInstanceOf[Calendar]
    _c.set(Calendar.DATE, c.getActualMaximum(Calendar.DAY_OF_MONTH))
    _c
  }

  /**
    * returns a date initialized at close date from calendar passed
    *
    * @param c {Calendar} calendar
    * @return {Date} modified date
    */
  def getCloseDateOf(c: Calendar): Date = {
    new Date(getCloseCalendarOf(c).getTimeInMillis)
  }

  /**
    * returns a timestamp initialized at close date from calendar passed
    *
    * @param c {Calendar} calendar
    * @return {Timestamp} modified timestamp
    */
  def getCloseTimestampOf(c: Calendar): Timestamp = {
    new Timestamp(getCloseCalendarOf(c).getTimeInMillis)
  }


}
