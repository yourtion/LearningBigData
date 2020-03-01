package com.yourtion.bigdata.c04

import org.apache.spark.sql.SparkSession

/**
 * DataSetAPI
 *
 * @see https://spark.apache.org/docs/latest/sql-getting-started.html#untyped-dataset-operations-aka-dataframe-operations
 */
object DataSetAPIApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSetAPIApp")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    // Encoders are created for case classes
    val ds = Seq(Person("Yourtion", "30")).toDS()
    ds.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(x => x + 1).collect().foreach(println)

    val peopleDF = spark.read.json("file:///tmp/people.json")
    val peopleDS = peopleDF.as[Person]
    peopleDS.show(false)

    // 运行期报错 peopleDF.select("anme").show()
    // 编译期报错 peopleDS.map(x => x.anme).show()
    peopleDS.map(x => x.name).show()

    spark.stop()
  }

  case class Person(name: String, age: String)

}
