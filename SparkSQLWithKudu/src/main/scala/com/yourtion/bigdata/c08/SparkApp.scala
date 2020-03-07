package com.yourtion.bigdata.c08

import com.yourtion.bigdata.c08.business.{LogETLProcessor, StatProvinceCityProcessor}
import org.apache.spark.sql.SparkSession


/**
 * 整个项目的入口点
 */
object SparkApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("SparkApp")
      .getOrCreate()

    // Step1: ETL
    LogETLProcessor.process(spark)
    // Step2: 省份地址
    StatProvinceCityProcessor.process(spark)

    spark.stop()
  }

}
