package com.yourtion.bigdata.c10

import org.apache.spark.sql.SparkSession

object JoinApp2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JoinApp2")
      .master("local")
      .getOrCreate()

    // 广播变量是把小表的数据通过sc广播出去
    val peopleInfo = spark.sparkContext
      .parallelize(Array(("100", "Yourtion"), ("101", "gyx"))).collectAsMap()
    val peopleBroadcast = spark.sparkContext.broadcast(peopleInfo)

    val peopleDetail = spark.sparkContext
      .parallelize(Array(("100", "ustc", "beijing"), ("103", "xxx", "shanghai")))
      .map(x => (x._1, x))

    // Spark 以及广播变量实现 join
    // mapPartitions做的事情： 遍历大表的每一行数据  和 广播变量的数据对比 有就取出来，没有就拉倒
    peopleDetail.mapPartitions(x => {
      val broadcastPeople = peopleBroadcast.value
      for ((key, value) <- x if broadcastPeople.contains(key))
        yield (key, broadcastPeople.getOrElse(key, ""), value._2)
    }).foreach(println)

    Thread.sleep(20000)

    spark.stop()
  }
}
