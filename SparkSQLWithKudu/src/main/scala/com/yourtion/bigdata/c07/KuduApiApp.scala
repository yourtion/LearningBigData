package com.yourtion.bigdata.c07

import java.util

import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client._
import org.apache.kudu.{ColumnSchema, Schema, Type}

object KuduApiApp {

  def main(args: Array[String]): Unit = {
    val KUDU_MASTERS = "yhost"
    val client = new KuduClientBuilder(KUDU_MASTERS).build()
    val tableName = "pk"
    val newTableName = "pk2"

    deleteTable(client, newTableName)

    createTable(client, tableName)

    insertRows(client, tableName)

    query(client, tableName)

    println("-----")
    alterRow(client, tableName)
    println("=====")

    query(client, tableName)

    renameTable(client, tableName, newTableName)

    client.close()
  }

  // 创建表
  def createTable(client: KuduClient, tableName: String): Unit = {
    // 实现Scala到Java的隐式转换
    import scala.collection.JavaConversions._
    val columns = List(
      new ColumnSchema.ColumnSchemaBuilder("word", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("cnt", Type.INT32).build()
    )
    val schema = new Schema(columns)

    val options = new CreateTableOptions()
    // 设置副本数
    options.setNumReplicas(1)
    // 配置分区键和分区用数
    val parCols = List("word")
    options.addHashPartitions(parCols, 3)

    client.createTable(tableName, schema, options)
  }

  // 插入记录
  def insertRows(client: KuduClient, tableName: String): Unit = {
    val table = client.openTable(tableName)
    val session = client.newSession()
    for (i <- 1 to 10) {
      val insert = table.newInsert()
      val row = insert.getRow
      row.addString("word", s"pk-$i")
      row.addInt("cnt", 100 + i)

      session.apply(insert)
    }
  }

  // 删除表
  def deleteTable(client: KuduClient, tableName: String): Unit = {
    client.deleteTable(tableName)
  }

  // 查询表
  def query(client: KuduClient, tableName: String): Unit = {
    val table = client.openTable(tableName)

    val scanner = client.newScannerBuilder(table).build()
    while (scanner.hasMoreRows) {
      val iterator = scanner.nextRows()
      while (iterator.hasNext) {
        val result = iterator.next()
        println(result.getString("word") + "=>" + result.getInt("cnt"))
      }
    }
  }

  // 更新记录
  def alterRow(client: KuduClient, tableName: String): util.List[OperationResponse] = {
    val table = client.openTable(tableName)
    val session = client.newSession()

    val update = table.newUpdate()
    val row = update.getRow
    row.addString("word", "pk-10")
    row.addInt("cnt", 8888)
    session.apply(update)
    session.close()
  }

  // 重命名表
  def renameTable(client: KuduClient, tableName: String, newTableName: String): AlterTableResponse = {
    val options = new AlterTableOptions()
    options.renameTable(newTableName)
    client.alterTable(tableName, options)
  }
}
