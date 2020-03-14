package com.yourtion.bigdata.spark.project.utils


import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间工具类
 */
object DateUtils {

  val SOURCE_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2017-10-22 14:46:01"))
  }

  def parseToMinute(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time: String): Long = {
    SOURCE_FORMAT.parse(time).getTime
  }
}
