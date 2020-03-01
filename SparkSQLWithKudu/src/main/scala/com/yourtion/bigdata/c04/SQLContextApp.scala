package com.yourtion.bigdata.c04

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContextApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQLContextApp")
      .setMaster("local")
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val df = sqlContext.read.text("file:///tmp/input.txt")
    df.show()
    sc.stop()
  }
}
