package com.yourtion.bigdata.spark.project

import com.yourtion.bigdata.spark.project.dao.{CourseClickCountDAO, CourseSearchCountDAO}
import com.yourtion.bigdata.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchCount}
import com.yourtion.bigdata.spark.project.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object WebStatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WebStatStreamingApp <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    // Spark Streaming 对接 Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    // 测试步骤一：测试数据接收
    // messages.map(_._2).count().print

    // 测试步骤二：数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      // line: 55.10.167.168	2020-03-14 16:39:43	"GET /class/130.html HTTP/1.1"	200	http://www.yahoo.com/search?p=理论
      val infos = line.split("\t")

      // infos(2) = "GET /class/130.html HTTP/1.1"
      // url = /class/130.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      // 把实战课程的课程编号拿到了
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clickLog => clickLog.courseId != 0)

    // cleanData.print()
    // 测试步骤三：统计今天到现在为止实战课程的访问量

    cleanData.map(x => {
      // HBase rowKey 设计： 20171111_88
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {
        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        CourseClickCountDAO.save(list)
      })
    })

    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    cleanData.map(x => {

      // https://www.sogou.com/web?query=Spark SQL实战 ==> https:/www.sogou.com/web?query=Spark SQL实战
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "")
      .map(x => (x._3.substring(0, 8) + "_" + x._1 + "_" + x._2, 1))
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partitionRecords => {
          val list = new ListBuffer[CourseSearchCount]

          partitionRecords.foreach(pair => {
            list.append(CourseSearchCount(pair._1, pair._2))
          })

          CourseSearchCountDAO.save(list)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
