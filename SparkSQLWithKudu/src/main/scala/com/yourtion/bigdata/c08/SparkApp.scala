package com.yourtion.bigdata.c08

import com.yourtion.bigdata.c08.business.StatAppProcessor
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
    //    LogETLProcessor.process(spark)
    // Step2: 省份地址统计
    //    StatProvinceCityProcessor.process(spark)
    // Step3: 地域分布情况统计
    //    StatAreaProcessor.process(spark)
    // Step4: APP分布情况统计
    StatAppProcessor.process(spark)
    spark.stop()
  }

}
