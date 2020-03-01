package com.yourtion.bigdata.c04

import org.apache.spark.sql.SparkSession

object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    // 读取文件 API
    val df = spark.read.text("file:///tmp/input.txt")
    df.show()

    spark.stop()
  }
}
