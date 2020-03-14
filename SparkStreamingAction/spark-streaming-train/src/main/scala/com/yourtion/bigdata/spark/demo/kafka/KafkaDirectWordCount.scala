package com.yourtion.bigdata.spark.demo.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming 对接 Kafka 的方式二（推荐）
 *
 * @see https://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html#approach-2-direct-approach-no-receivers
 */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf() //.setMaster("local[2]").setAppName("KafkaDirectWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Spark Streaming 对接 Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )
    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
