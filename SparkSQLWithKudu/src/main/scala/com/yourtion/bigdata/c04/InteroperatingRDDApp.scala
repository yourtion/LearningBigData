package com.yourtion.bigdata.c04

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Interoperating with RDDs
 *
 * @see https://spark.apache.org/docs/latest/sql-getting-started.html#interoperating-with-rdds
 */
object InteroperatingRDDApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    // Inferring the Schema Using Reflection
    runInferSchema(spark)
    // Programmatically Specifying the Schema
    runProgrammaticSchema(spark)


    spark.stop()
  }

  /**
   * 第二种方式：自定义编程
   */
  def runProgrammaticSchema(spark: SparkSession): Unit = {

    // 1. Create an RDD of Rows from the original RDD;
    val peopleRDD = spark.sparkContext.textFile("file:///tmp/people.txt")
    val peopleRowRDD = peopleRDD.map(_.split(","))
      .map(x => Row(x(0), x(1).trim.toInt))

    // 2. Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    val struct =
      StructType(
        StructField("name", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = false) :: Nil)
    // 3. Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
    val peopleDF = spark.createDataFrame(peopleRowRDD, struct)
    peopleDF.show(false)
  }

  /**
   * 第一种方式：反射
   * 1）定义case class
   * 2）RDD map，map中每一行数据转成case class
   */
  def runInferSchema(spark: SparkSession): Unit = {
    import spark.implicits._

    // 读取文件 API
    val peopleRDD = spark.sparkContext.textFile("file:///tmp/people.txt")

    // RDD => DF
    val peopleDF = peopleRDD.map(_.split(","))
      .map(x => People(x(0), x(1).trim.toInt))
      .toDF()
    peopleDF.show(false)
    peopleDF.createOrReplaceTempView("people1")
    val s1 = spark.sql("select name,age from people1 where age between 19 and 29")
    s1.show(false)
    // From index
    s1.map(x => "Name:" + x(0)).show()
    // From field
    s1.map(x => "Name2:" + x.getAs[String]("name")).show()
  }

  case class People(name: String, age: Int)

}
