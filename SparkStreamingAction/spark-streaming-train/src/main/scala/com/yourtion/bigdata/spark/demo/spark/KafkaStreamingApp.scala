package com.yourtion.bigdata.spark.demo.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 对接 Kafka 的方式二（推荐）
 *
 * @see https://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html#approach-2-direct-approach-no-receivers
 */
object KafkaStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: KafkaStreamingApp <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaStreamingApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Spark Streaming 对接 Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )
    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
