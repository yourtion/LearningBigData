package com.yourtion.bigdata.spark.project.domain

/**
 * 实战课程点击数实体类
 *
 * @param day_course  对应的就是 HBase 中的 rowKey，20171111_1
 * @param click_count 对应的 20171111_1 的访问总数
 */
case class CourseClickCount(day_course: String, click_count: Long)
