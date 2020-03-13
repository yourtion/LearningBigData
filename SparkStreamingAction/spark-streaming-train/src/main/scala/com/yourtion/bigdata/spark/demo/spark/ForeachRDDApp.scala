package com.yourtion.bigdata.spark.demo.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
 *
 * create table word_count(word varchar(64) default null,word_count int(11) default 0);
 * select * from word_count;
 */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // result.print()  //此处仅仅是将统计结果输出到控制台

    // 将结果写入到MySQL
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        // TODO 使用连接池获取 MySQL 连接
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          // TODO 一次插入多条数据，使用 UPSERT 保证单词累加
          val sql = "insert into word_count(word, word_count) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 获取MySQL的连接
   */
  def createConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://yhost:3306/spark?useSSL=false", "root", "1qazMySQL!!!")
  }

}
