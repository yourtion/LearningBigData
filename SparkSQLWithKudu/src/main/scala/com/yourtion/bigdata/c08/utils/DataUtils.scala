package com.yourtion.bigdata.c08.utils

import org.apache.spark.sql.SparkSession

object DataUtils {
  def getTableName(tableName: String, spark: SparkSession): String = {
    val time = spark.sparkContext.getConf.getOption("spark.time") getOrElse "20181007"
    tableName + "_" + time
  }

  def getRawPath(spark: SparkSession): String = {
    val rawPath = spark.sparkContext.getConf.getOption("spark.raw.path")
    rawPath getOrElse "/tmp/data-test.json"
  }

  def getIpPath(spark: SparkSession): String = {
    val ipRulePath = spark.sparkContext.getConf.getOption("spark.ip.path")
    ipRulePath getOrElse "/tmp/ip.txt"
  }

  def getKuduMaster(spark: SparkSession): String = {
    val master = spark.sparkContext.getConf.getOption("spark.kudu.master")
    master getOrElse "yhost"
  }

}
