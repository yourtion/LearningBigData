package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.utils.{KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object StatProvinceCityApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("StatProvinceCityApp")
      .getOrCreate()

    // 从KUDU的ods表中读取数据，然后进行按照省份和城市分组统计即可
    val masterAddresses = "yhost"
    val sourceTable = "ods"

    val odsDF = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTable)
      .option("kudu.master", masterAddresses)
      .load()
    // odsDF.show()
    odsDF.createOrReplaceTempView("ods")
    val result = spark.sql(SQLUtils.PROVINCE_CIY_SQL)
    // result.show(false)
    val resultTable = "stat_province_city"
    val partitionId = "province"
    KuduUtils.sink(result, resultTable, masterAddresses, SchemaUtils.ProvinceCitySchema, partitionId)

    spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", resultTable)
      .option("kudu.master", masterAddresses)
      .load().show(false)

    spark.stop()
  }
}
