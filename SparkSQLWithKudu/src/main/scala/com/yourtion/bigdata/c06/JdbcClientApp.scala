package com.yourtion.bigdata.c06

import java.sql.DriverManager

object JdbcClientApp {
  def main(args: Array[String]): Unit = {

    // 加载驱动
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    // 读取数据
    val conn = DriverManager.getConnection("jdbc:hive2://yhost:10000")
    val pstmt = conn.prepareStatement("select * from pk")
    val rs = pstmt.executeQuery()

    while (rs.next()) {
      println(rs.getObject(1) + " : " + rs.getObject(2))
    }
  }

}
