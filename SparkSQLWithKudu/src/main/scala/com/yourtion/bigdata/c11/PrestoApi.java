package com.yourtion.bigdata.c11;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class PrestoApi {
    public static void main(String[] args) {
        try {
            Class.forName("com.facebook.presto.jdbc.PrestoDriver");

            Connection connection = DriverManager.getConnection("jdbc:presto://yhost:8080/hive/default", "hadoop", "");
            Statement statement = connection.createStatement();

            // 显示表
            ResultSet resultSet = statement.executeQuery("show tables");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
            }

            // 查找记录
            ResultSet resultSet1 = statement.executeQuery("select * from pk");
            while (resultSet1.next()) {
                System.out.println(resultSet1.getInt("id") + "\t" + resultSet1.getString("name"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
