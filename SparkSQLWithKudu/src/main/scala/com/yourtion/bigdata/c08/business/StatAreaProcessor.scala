package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.`trait`.DataProcess
import com.yourtion.bigdata.c08.utils.{DataUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object StatAreaProcessor extends DataProcess {
  override def process(spark: SparkSession): Unit = {
    // 从KUDU的ods表中读取数据，然后进行按照省份和城市分组统计即可
    val masterAddresses = DataUtils.getKuduMaster(spark)
    val sourceTable = DataUtils.getTableName("ods", spark)

    val odsDF = KuduUtils.load(spark, masterAddresses, sourceTable)
    // odsDF.show()
    odsDF.createOrReplaceTempView("ods")
    val resultTmp = spark.sql(SQLUtils.AREA_SQL_STEP1)
    resultTmp.createOrReplaceTempView("area_tmp")
    val result = spark.sql(SQLUtils.AREA_SQL_STEP2)
    // result.show(false)

    val resultTable = DataUtils.getTableName("stat_area", spark)
    val partitionId = "province"
    KuduUtils.sink(result, resultTable, masterAddresses, SchemaUtils.AREASchema, partitionId)

    KuduUtils.load(spark, masterAddresses, resultTable).show(false)
  }
}
