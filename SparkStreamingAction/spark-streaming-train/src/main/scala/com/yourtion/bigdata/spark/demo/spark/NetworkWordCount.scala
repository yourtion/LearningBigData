package com.yourtion.bigdata.spark.demo.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming处理Socket数据
 * $ nc -lk 6789
 */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    // 必须开启两个进程以上！！！
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    // 创建 StreamingContext 需要两个参数：SparkConf 和 batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = {
      ssc.socketTextStream("localhost", 6789)
    }

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
