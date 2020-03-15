package com.yourtion.bigdata.spark.project.web;

import com.yourtion.bigdata.spark.project.web.dao.CourseClickCountDAO;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author yourtion
 */
@SpringBootApplication
public class SparkStreamingWebApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(SparkStreamingWebApplication.class, args);
        // 预热 HBase
        context.getBean(CourseClickCountDAO.class).getTable();
    }

}
