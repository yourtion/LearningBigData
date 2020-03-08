package com.yourtion.bigdata.c10

import org.apache.spark.sql.SparkSession

object JoinApp1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JoinApp1")
      .master("local")
      .getOrCreate()

    val peopleInfo = spark.sparkContext
      .parallelize(Array(("100", "Yourtion"), ("101", "gyx")))
      .map(x => (x._1, x))
    val peopleDetail = spark.sparkContext
      .parallelize(Array(("100", "ustc", "beijing"), ("103", "xxx", "shanghai")))
      .map(x => (x._1, x))

    peopleInfo.join(peopleDetail).map(x => x._1 + " : " + x._2._1._2 + " : " + x._2._2._2).foreach(println)

    Thread.sleep(20000)

    spark.stop()
  }
}
