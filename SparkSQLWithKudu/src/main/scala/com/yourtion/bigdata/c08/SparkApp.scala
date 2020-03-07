package com.yourtion.bigdata.c08

import com.yourtion.bigdata.c08.business._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession


/**
 * 整个项目的入口点
 * 1） spark.time
 * 2） spark.raw.path
 * 3） spark.ip.path
 *
 * @example spark-submit ......  --conf spark.time=20181007 spark.raw.path=zzz spark.ip.path=xxx
 */
object SparkApp extends Logging {

  def main(args: Array[String]): Unit = {

    val local = args.length > 0 && args(0) == "debug"

    val builder = SparkSession.builder()
    if (local) {
      println("Run debug mode")
      builder.master("local[2]").appName("SparkAppLocal")
    }
    val spark = builder.getOrCreate()
    // spark框架只认以spark.开头的参数，否则系统不识别
    val time = spark.sparkContext.getConf.getOption("spark.time")
    // 如果是空，后续的代码就不应该执行了
    if (local && time == null) {
      logError("处理批次不能为空....")
      System.exit(0)
    }

    // Step1: ETL
    LogETLProcessor.process(spark)
    // Step2: 省份地址统计
    StatProvinceCityProcessor.process(spark)
    // Step3: 地域分布情况统计
    StatAreaProcessor.process(spark)
    // Step4: APP分布情况统计
    StatAppProcessor.process(spark)

    spark.stop()
  }

}
