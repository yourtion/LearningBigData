package com.yourtion.bigdata.spark.demo.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 黑名单过滤
 *
 * $ nc -lk 6789
 * 20200101,zs
 */
object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 构建黑名单
    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6789)
    // rdd ==> (zs: 20200101,zs)(ww: 20200101,ww)
    //     ==> (zs: [<20200101,zs>, <true>])(ww: [<20200101,ww>, <false>])
    //     ==> (20200101,ww)
    val clickLog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => !x._2._2.getOrElse(false))
        .map(x => x._2._1)
    })
    clickLog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

