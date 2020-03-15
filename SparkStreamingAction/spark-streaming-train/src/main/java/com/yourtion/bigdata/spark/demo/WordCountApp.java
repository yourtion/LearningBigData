package com.yourtion.bigdata.spark.demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 使用 Java 进行 WordCount
 *
 * @author yourtion
 */
public class WordCountApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WordCountApp").master("local").getOrCreate();

        // 业务逻辑
        JavaRDD<String> lines = spark.read().textFile("/tmp/hello.txt").javaRDD();

        JavaRDD<String> words = lines.flatMap(line ->
                Arrays.asList(line.split(",")).iterator());
        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String, Integer> tuple : output) {
            System.out.println(tuple._1() + " : " + tuple._2());
        }

        spark.stop();
    }
}
