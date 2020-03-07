package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.`trait`.DataProcess
import com.yourtion.bigdata.c08.utils.{KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object StatAppProcessor extends DataProcess {
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
    val resultTmp = spark.sql(SQLUtils.APP_SQL_STEP1)
    resultTmp.createOrReplaceTempView("app_tmp")
    val result = spark.sql(SQLUtils.APP_SQL_STEP2)
    result.show(false)

    val resultTable = "stat_app"
    val partitionId = "app_id"
    KuduUtils.sink(result, resultTable, masterAddresses, SchemaUtils.APPSchema, partitionId)

    spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.table", resultTable)
      .option("kudu.master", masterAddresses)
      .load().show(false)
  }
}
