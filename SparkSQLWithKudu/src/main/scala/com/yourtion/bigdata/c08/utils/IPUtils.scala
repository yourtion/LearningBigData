package com.yourtion.bigdata.c08.utils

object IPUtils {

  def main(args: Array[String]): Unit = {
    println(ip2Long("182.91.190.221"))
  }

  /**
   * 将字符串转成十进制
   *
   * @param ip IP地址
   */
  def ip2Long(ip: String): Long = {
    val splits = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until splits.length) {
      ipNum = splits(i).toLong | ipNum << 8L
    }
    ipNum
  }
}
