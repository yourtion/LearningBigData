package com.yourtion.bigdata.spark.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用 Java 进行 SparkStreaming WordCount
 * nc -lk 6789
 *
 * @author yourtion
 */
public class StreamingWordCountApp {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCountApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 创建 DStream
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 6789);
        JavaPairDStream<String, Integer> counts = lines.flatMap(line ->
                Arrays.asList(line.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum);

        counts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}
