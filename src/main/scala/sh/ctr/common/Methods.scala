package sh.ctr.common

import Constant.APP_ID_MAP
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.EMPTY

object Methods {

  def getAppIds(platform: String): Array[String] = APP_ID_MAP.getOrElse(platform, Array[String](""))

  def getView(table: String, dt: String, platform: String): String = Array[String](table, platform, dt).mkString("_")

  def getArg(args: Map[String, String], key: String): String = {
    val v = args.getOrElse(key, EMPTY)
    if (v == null) {
      return EMPTY
    }
    v
  }

  def getLongArg(args: Map[String, String], key: String): Long = {
    val v = args.getOrElse(key, EMPTY)
    if (StringUtils.isBlank(v)) {
      return 0L
    }
    v.toLong
  }

  def getDoubleArg(args: Map[String, String], key: String): Double = {
    val v = args.getOrElse(key, EMPTY)
    if (StringUtils.isBlank(v)) {
      return 0D
    }
    v.toDouble
  }

  def getIntArg(args: Map[String, String], key: String): Int = {
    val v = args.getOrElse(key, EMPTY)
    if (StringUtils.isBlank(v)) {
      return 0
    }
    v.toInt
  }
}
