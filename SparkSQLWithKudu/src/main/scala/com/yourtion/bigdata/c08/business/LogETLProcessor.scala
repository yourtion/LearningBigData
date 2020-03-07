package com.yourtion.bigdata.c08.business

import com.yourtion.bigdata.c08.`trait`.DataProcess
import com.yourtion.bigdata.c08.utils._
import org.apache.spark.sql.SparkSession


/**
 * 日志ETL清洗操作
 */
object LogETLProcessor extends DataProcess {
  override def process(spark: SparkSession): Unit = {
    // 使用 Data Source API 加载 json 数据

    val rawPath = DataUtils.getRawPath(spark)
    var jsonDF = spark.read.json(rawPath)
    // jsonDF.printSchema()
    // jsonDF.show(false)

    import spark.implicits._
    val ipRulePath = DataUtils.getIpPath(spark)
    val ipRowRDD = spark.sparkContext.textFile(ipRulePath)
    // 建议使用 DF 需要将 RDD => DF 的相关操作，或者 DF 注册成表，然后进行相关操作
    val ipRuleDF = ipRowRDD.map(x => {
      val splits = x.split("\\|")
      val startIP = splits(2).toLong
      val endIP = splits(3).toLong
      val province = splits(6)
      val city = splits(7)
      val isp = splits(9)
      (startIP, endIP, province, city, isp)
    }).toDF("start_ip", "end_ip", "province", "city", "isp")
    // ipRuleDF.show(false)

    // 将每一行日志中的 IP 获得到对应到省份、城市、运营商

    // 注册 UDF 函数
    import org.apache.spark.sql.functions._
    def getLongIp = udf((ip: String) => IPUtils.ip2Long(ip))

    jsonDF = jsonDF.withColumn("ip_long", getLongIp($"ip"))
    // 两个 DF 进行 join，条件是 json 中的 IP 是在规则 IP 中的范围内（between ... and ...）
    // jsonDF.join(ipRuleDF, jsonDF("ip_long").between(ipRuleDF("start_ip"), ipRuleDF("end_ip"))).show(false)

    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")
    val result = spark.sql(SQLUtils.SQL)
    //.show(false)

    val masterAddresses = DataUtils.getKuduMaster(spark)
    val table = DataUtils.getTableName("ods", spark)
    val partitionId = "ip"
    KuduUtils.sink(result, table, masterAddresses, SchemaUtils.ODSSchema, partitionId)

    KuduUtils.load(spark, masterAddresses, table).show()
  }
}
