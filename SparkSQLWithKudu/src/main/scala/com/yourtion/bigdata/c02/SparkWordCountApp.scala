package com.yourtion.bigdata.c02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 词频统计案例
 *
 * 输入：文件
 * 需求：统计出文件中每个单词出现的次数
 *      1. 读每一行数据
 *      2. 按照分隔符把每一行的数据拆成单词
 *      3. 每个单词赋上次数为1
 *      4. 按照单词进行分发，然后统计单词出现的次数
 *      5. 把结果输出到文件中
 * 输出：文件
 */
object SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile(args(0))
    rdd.flatMap(_.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
      .saveAsTextFile(args(1))

    sc.stop()
  }
}

/*
spark-submit \
 --class com.yourtion.bigdata.c02.SparkWordCountApp \
 --master yarn \
 --name SparkWordCountApp \
 sparksql-train-1.0.jar \
 hdfs://yhost:8020/tmp/input.txt hdfs://yhost:8020/tmp/out
 */
