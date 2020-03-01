package com.yourtion.bigdata.c06

import org.apache.spark.sql.SparkSession

/**
 * 自定义函数
 * 1）定义函数
 * 2）注册函数
 * 3）使用函数
 */
object UDFFunctionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val infoRDD = spark.sparkContext.textFile("file:///tmp/hobbies.txt")
    val infoDF = infoRDD.map(_.split("###")).map(x => Hobbies(x(0), x(1))).toDF()
    infoDF.show(false)

    // 定义函数 + 注册函数
    spark.udf.register("hobby_num", (s: String) => s.split(",").length)

    // 使用函数 select name, hobby_num(hobbies) from xxx
    infoDF.createOrReplaceTempView("info")
    spark.sql("select name, hobby_num(hobbies) as hobby_count from info").show()


    spark.stop()
  }

  case class Hobbies(name: String, hobbies: String)
}
