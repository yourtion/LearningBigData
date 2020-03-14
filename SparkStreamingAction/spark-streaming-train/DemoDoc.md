# Demo 运行方法

## FlumePushWordCount

Spark Streaming 整合 Flume 的第一种方式 Push

https://spark.apache.org/docs/latest/streaming-flume-integration.html#approach-1-flume-style-push-based-approach

```bash
// 1. 启动 SparkStreaming
$ spark-submit \
      --class com.yourtion.bigdata.spark.demo.flume.FlumePushWordCount \
      --master local[2] \
      --packages org.apache.spark:spark-streaming-flume_2.11:2.4.5 \
      spark-streaming-train-1.0.jar \
      localhost 41414

// 2. 启动 Flume
$ flume-ng agent \
    --conf $FLUME_HOME/conf \
    --conf-file flume-push-streaming.conf \
    --name flume-push-streaming \
    -Dflume.root.logger=INFO,console

// 3. 使用 telnet 发数据
$ telnet localhost 44444
```

## FlumePullWordCount

Spark Streaming 整合 Flume 的第二种方式 Pull（推荐）

https://spark.apache.org/docs/latest/streaming-flume-integration.html#approach-2-pull-based-approach-using-a-custom-sink

```bash
// 1. 启动 Flume
$ flume-ng agent \
    --conf $FLUME_HOME/conf \
    --conf-file flume-pull-streaming.conf \
    --name flume-pull-streaming \
    -Dflume.root.logger=INFO,console

// 2. 启动 SparkStreaming
$ spark-submit \
      --class com.yourtion.bigdata.spark.demo.flume.FlumePullWordCount \
      --master local[2] \
      --packages org.apache.spark:spark-streaming-flume_2.11:2.4.5 \
      spark-streaming-train-1.0.jar \
      localhost 42424

// 3. 使用 telnet 发数据
$ telnet localhost 44444
```
