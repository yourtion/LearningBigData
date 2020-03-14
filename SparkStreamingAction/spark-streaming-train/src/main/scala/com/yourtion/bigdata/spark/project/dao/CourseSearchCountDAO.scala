package com.yourtion.bigdata.spark.project.dao

import com.yourtion.bigdata.spark.project.domain.CourseSearchCount
import com.yourtion.bigdata.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
 * 从搜索引擎过来的实战课程点击数-数据访问层
 */
object CourseSearchCountDAO {

  val tableName = "imooc_course_search_count"
  val cf = "info"
  val qualifier = "click_count"

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchCount]
    list.append(CourseSearchCount("20200101_www.baidu.com_8", 8))
    list.append(CourseSearchCount("20200101_cn.bing.com_9", 9))

    save(list)

    println(count("20200101_www.baidu.com_8") + " : " + count("20200101_cn.bing.com_9"))
  }

  /**
   * 保存数据到HBase
   *
   * @param list CourseSearchClickCount集合
   */
  def save(list: ListBuffer[CourseSearchCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count)
    }

  }

  /**
   * 根据 rowKey 查询值
   */
  def count(day_search_course: String): Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_search_course))
    val value = table.get(get).getValue(cf.getBytes, qualifier.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

}
