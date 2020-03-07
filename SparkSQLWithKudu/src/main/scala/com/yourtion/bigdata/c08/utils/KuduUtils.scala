package com.yourtion.bigdata.c08.utils

import org.apache.kudu.Schema
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.spark.sql.{DataFrame, SaveMode}

object KuduUtils {

  /**
   * 将DF数据落地到Kudu
   *
   * @param data        DataFrame结果集
   * @param tableName   Kudu目标表
   * @param master      Kudu的Master地址
   * @param schema      Kudu表的schema信息
   * @param partitionId Kudu表的分区字段
   */
  def sink(data: DataFrame,
           tableName: String,
           master: String,
           schema: Schema,
           partitionId: String
          ): Unit = {
    val client = new KuduClientBuilder(master).build()

    import scala.collection.JavaConversions._
    val options = new CreateTableOptions()
    options.setNumReplicas(1)
    val parCols = List(partitionId)
    options.addHashPartitions(parCols, 3)

    // 创建表
    if (client.tableExists(tableName)) {
      println("delete table: " + tableName)
      client.deleteTable(tableName)
    }
    client.createTable(tableName, schema, options)

    // 数据写入KUDU
    data.write.mode(SaveMode.Append)
      .format("org.apache.kudu.spark.kudu")
      .option("kudu.table", tableName)
      .option("kudu.master", master)
      .save()
  }

}
