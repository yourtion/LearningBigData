package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.`trait`.DataProcess
import com.yourtion.bigdata.c08.utils.{DataUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.SparkSession

object StatProvinceCityProcessor extends DataProcess {

  override def process(spark: SparkSession): Unit = {
    val masterAddresses = DataUtils.getKuduMaster(spark)
    val sourceTable = DataUtils.getTableName("ods", spark)

    val odsDF = KuduUtils.load(spark, masterAddresses, sourceTable)
    // odsDF.show()
    odsDF.createOrReplaceTempView("ods")
    val result = spark.sql(SQLUtils.PROVINCE_CIY_SQL)
    // result.show(false)
    val resultTable = DataUtils.getTableName("stat_province_city", spark)
    val partitionId = "province"
    KuduUtils.sink(result, resultTable, masterAddresses, SchemaUtils.ProvinceCitySchema, partitionId)

    KuduUtils.load(spark, masterAddresses, resultTable).show()
  }
}
