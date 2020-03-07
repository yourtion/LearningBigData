package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.`trait`.DataProcess
import com.yourtion.bigdata.c08.utils.{KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object StatAreaProcessor extends DataProcess {
  override def process(spark: SparkSession): Unit = {
    // 从KUDU的ods表中读取数据，然后进行按照省份和城市分组统计即可
    val masterAddresses = "yhost"
    val sourceTable = "ods"

    val odsDF = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", sourceTable)
      .option("kudu.master", masterAddresses)
      .load()
    // odsDF.show()
    odsDF.createOrReplaceTempView("ods")
    val resultTmp = spark.sql(SQLUtils.AREA_SQL_STEP1)
    resultTmp.createOrReplaceTempView("area_tmp")
    val result = spark.sql(SQLUtils.AREA_SQL_STEP2)
    // result.show(false)

    val resultTable = "stat_area"
    val partitionId = "province"
    KuduUtils.sink(result, resultTable, masterAddresses, SchemaUtils.AREASchema, partitionId)

    spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", resultTable)
      .option("kudu.master", masterAddresses)
      .load().show(false)
  }
}
