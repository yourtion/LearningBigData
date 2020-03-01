package com.yourtion.bigdata.c04

import org.apache.spark.sql.SparkSession

/**
 * DataFrameAPI
 *
 * @see https://spark.apache.org/docs/latest/sql-getting-started.html#creating-dataframes
 */
object DataFrameAPIApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameAPIApp")
      .master("local")
      .getOrCreate()

    // 读取文件 API
    val people = spark.read.json("file:///tmp/people.json")
    // 查看 DF 内部结构
    people.printSchema()
    // 展示 DF 内部的数据
    people.show()

    // 仅展示 name 列 ==> select name from people
    people.select("name").show()
    // 使用隐式转换 import spark.implicits._
    import spark.implicits._
    people.select($"name").show()

    // select * from people where age > 21
    people.filter($"age" > 21).show()
    people.filter("age > 21").show()

    // select age, count(1) from people group by age
    people.groupBy("age").count().show()

    // select name, age+10 from people
    people.select($"name", ($"age" + 10).as("new_age")).show()

    // 使用 SQL 方式操作
    people.createOrReplaceTempView("people")
    spark.sql("select name from people where age > 21").show()

    // 加载新数据
    val zips = spark.read.json("file:///tmp/zips.json")
    zips.printSchema()
    zips.show(false)
    zips.head(3).foreach(println)
    // zips.first()
    // zips.take(5)

    val count = zips.count()
    println(s"Total Counts: $count")

    // 过滤出 pop > 40000 的
    zips.filter(zips.col("pop") > 40000)
      .withColumnRenamed("_id", "id")
      .show(10, truncate = false)

    // 统计加州 pop 最多的10个城市名称和ID
    import org.apache.spark.sql.functions._
    zips
      .select("_id", "city", "pop", "state")
      .filter(zips.col("state") === "CA")
      .orderBy(desc("pop"))
      .show(10, truncate = false)

    zips.createOrReplaceTempView("zips")
    spark.sql("select _id,city,pop,state from zips where state='CA' order by pop desc limit 10").show(false)

    spark.stop()
  }
}
