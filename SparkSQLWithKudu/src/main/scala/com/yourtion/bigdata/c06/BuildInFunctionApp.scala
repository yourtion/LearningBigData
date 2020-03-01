package com.yourtion.bigdata.c06

import org.apache.spark.sql.SparkSession

object BuildInFunctionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val userAccessLog = Array(
      "2016-10-01,1122", // day  userId
      "2016-10-01,1122",
      "2016-10-01,1123",
      "2016-10-01,1124",
      "2016-10-01,1124",
      "2016-10-02,1122",
      "2016-10-02,1121",
      "2016-10-02,1123",
      "2016-10-02,1123"
    )
    // Array ==> RDD
    import spark.implicits._
    val userAccessRDD = spark.sparkContext.parallelize(userAccessLog)
    val userAccessDF = userAccessRDD.map(x => {
      val splits = x.split(",")
      Log(splits(0), splits(1).toInt)
    }).toDF()
    userAccessDF.show(false)

    import org.apache.spark.sql.functions._
    // select day, count(user_id) from xxx group by day;
    userAccessDF.groupBy("day").agg(count("userId").as("pv")).show()
    // select day, count(distinct user_id) from xxx group by day;
    userAccessDF.groupBy("day").agg(countDistinct("userId").as("uv")).show()

    spark.stop()
  }

  case class Log(day: String, userId: Int)

}
