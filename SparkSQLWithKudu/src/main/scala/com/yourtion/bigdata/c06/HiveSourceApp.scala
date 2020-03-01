package com.yourtion.bigdata.c06

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object HiveSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      // 一定要开启Hive支持
      .enableHiveSupport()
      .getOrCreate()

    spark.table("pk").show()

    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val database = config.getString("db.default.database")
    val table = config.getString("db.default.table1")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)

    val jdbcDF = spark.read
      .jdbc(url, s"$database.$table", connectionProperties)

    jdbcDF.show()
    jdbcDF.write.mode("overwrite").saveAsTable(table + "1")
    spark.table(table+ "1").show()

    // saveAsTable和insertInto的区别
    jdbcDF.write.insertInto(table + "3")
    spark.table(table+ "3").show()

    spark.stop()
  }
}
