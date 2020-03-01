package com.yourtion.bigdata.c05

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataSourceApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    text(spark)
    json(spark)
    common(spark)
    parquet(spark)
    convert(spark)
    jdbc(spark)
    jdbc2(spark)

    spark.stop()
  }

  // 从配置文件读取数据
  def jdbc2(spark: SparkSession): Unit = {
    println("----- jdbc2 -----")

    val config = ConfigFactory.load()
    val url = config.getString("db.default.url")
    val user = config.getString("db.default.user")
    val password = config.getString("db.default.password")
    val db = config.getString("db.default.database")
    val table1 = config.getString("db.default.table1")
    val table2 = config.getString("db.default.table2")

    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    val jdbcDF2 = spark.read
      .jdbc(url, s"$db.$table1", connectionProperties)
    jdbcDF2.show(false)
    jdbcDF2.write.mode("overwrite").jdbc(url, s"$db.$table2", connectionProperties)
  }

  // 数据库读取
  def jdbc(spark: SparkSession): Unit = {
    println("----- jdbc -----")

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://yhost:3306?useSSL=false")
      .option("dbtable", "hive.TBLS")
      .option("user", "root")
      .option("password", "1qazMySQL!!!")
      .load()
    jdbcDF.show(false)
    jdbcDF.filter("TBL_ID > 1").show()
  }


  // 存储类型转换 JSON ==> Parquet
  def convert(spark: SparkSession): Unit = {
    println("----- convert -----")

    val jsonDF = spark.read.format("json").load("file:///tmp/people.json")
    jsonDF.show(false)
    jsonDF.write.format("parquet").mode("overwrite").save("file:///tmp/people_out_parquet")

    val parquetDF = spark.read.load("file:///tmp/people_out_parquet")
    parquetDF.filter("age > 20").show(false)
  }

  // Parquet
  def parquet(spark: SparkSession): Unit = {
    println("----- parquet -----")

    //    val userDF = spark.read.parquet("file:///tmp/user.parquet")
    val parquetDF = spark.read.load("file:///tmp/users.parquet")
    parquetDF.printSchema()
    parquetDF.show(false)

    parquetDF.select("name", "favorite_numbers")
      .write.mode("overwrite")
      // 关闭压缩
      .option("compression", "none")
      .parquet("file:///tmp/users_out_parquet")
  }

  // 标准写法
  def common(spark: SparkSession): Unit = {
    println("----- common -----")

    val textDF = spark.read.format("text").load("file:///tmp/people.txt")
    val jsonDF = spark.read.format("json").load("file:///tmp/people.json")
    textDF.show(false)
    jsonDF.show(false)
    jsonDF.write.format("json").mode("overwrite").save("file:///tmp/people_out")
  }

  // JSON
  def json(spark: SparkSession): Unit = {
    println("----- json -----")

    val jsonDF = spark.read.json("file:///tmp/people.json")
    jsonDF.show(false)
    // 只要 age > 20
    jsonDF.filter("age > 20")
      .write.mode(SaveMode.Overwrite)
      .json("file:///tmp/people_out_json")

    import spark.implicits._

    val jsonDF2 = spark.read.json("file:///tmp/people2.json")
    jsonDF2.show(false)
    jsonDF2.select($"name", $"age", $"info.work".as("work"), $"info.home".as("home"))
      .write.mode(SaveMode.Append)
      .json("file:///tmp/people_out_json")

  }

  // 文本
  def text(spark: SparkSession): Unit = {
    println("----- text -----")

    val textDF = spark.read.text("file:///tmp/people.txt")
    textDF.show(false)

    import spark.implicits._

    val result = textDF.map(x => {
      val splits = x.getString(0).split(",")
      splits(0).trim + "|" + splits(1).trim
    })
    // 配置保存模式
    // result.write.mode("overwrite").text("file:///tmp/people_out_text")
    result.write.mode(SaveMode.Append).text("file:///tmp/people_out_text")
  }

}
