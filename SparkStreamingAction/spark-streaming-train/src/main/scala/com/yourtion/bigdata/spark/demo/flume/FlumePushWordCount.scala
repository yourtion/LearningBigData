package com.yourtion.bigdata.spark.demo.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 整合 Flume 的第一种方式 Push
 *
 * @see https://spark.apache.org/docs/latest/streaming-flume-integration.html#approach-1-flume-style-push-based-approach
 *
 * 1. 启动 SparkStreaming 程序
 * 2. Flume 启动 flume-push-streaming.conf
 * 3. telnet 连接 Flume netcat
 */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 使用SparkStreaming整合Flume
    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
