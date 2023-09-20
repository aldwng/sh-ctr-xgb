package sh.ctr.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Utils {
  lazy val DATE_FORMAT = new SimpleDateFormat("yyyyMMdd")
  lazy val DATE_FORMAT_FULL = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss")

  def lastDates(days: Int): ArrayBuffer[String] = {
    lastDates(days, DATE_FORMAT.format(new Date()))
  }

  private def lastDates(days: Int, date: String): ArrayBuffer[String] = {
    val arr = ArrayBuffer[String]()
    for (i <- 1 to days) {
      arr.append(someDay(date, i * (-1)))
    }
    arr
  }

  def yesterday(): String = {
    val today = DATE_FORMAT.format(new Date())

    someDay(today, -1)
  }

  def today(): String = {
    DATE_FORMAT.format(new Date())
  }

  def lastDays(period: Int): String = {
    val today = DATE_FORMAT.format(new Date())
    someDay(today, -period)
  }

  def someDay(from: String, days: Int): String = {
    if (days == 0) {
      return from
    }

    val date = DATE_FORMAT.parse(from)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, days)

    DATE_FORMAT.format(cal.getTime)
  }

  def parseArgs(args: Array[String]): mutable.Map[String, String] = {
    val range = new Range(0, args.length, 2)
    val ret = mutable.Map[String, String]()

    for (idx <- range) {
      val k = args(idx)
      ret += (k -> args(idx + 1))
    }
    ret
  }
}
