package com.yourtion.bigdata.spark.project.dao

import com.yourtion.bigdata.spark.project.domain.CourseClickCount
import com.yourtion.bigdata.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 实战课程点击数-数据访问层
 */
object CourseClickCountDAO {

  val tableName = "imooc_course_click_count"
  val cf = "info"
  val qualifier = "click_count"

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20200101_8", 8))
    list.append(CourseClickCount("20200101_9", 9))
    list.append(CourseClickCount("20200101_1", 100))

    save(list)

    println(count("20200101_8") + " : " + count("20200101_9") + " : " + count("20200101_1"))
  }

  /**
   * 保存数据到HBase
   *
   * @param list CourseClickCount集合
   */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count)
    }
  }

  /**
   * 根据 RowKey 查询值
   */
  def count(day_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifier.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }
}
