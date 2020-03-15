# Spark Streaming实时流处理项目实战

Flume+Kafka+Spark Streaming打造通用流处理平台

## 环境准备

### HBase

```shell
// 创建表，RowKey 设置（day_courseid）
> create 'imooc_course_click_count','info'
// 创建表，RowKey 设置（day_search_courseid）
> create 'imooc_course_search_count','info'
// 清空数据
> truncate 'imooc_course_click_count'
> truncate 'imooc_course_search_count'
```

## 运行

```
// 生成日志
$ python web_log_generate.py 500 /tmp/access.log

// Flume 日志采集
$ flume-ng agent \
    --conf $FLUME_HOME/conf \
    --conf-file streaming-project.conf \
    --name streaming-project \
    -Dflume.root.logger=INFO,console

// 启动 SparkStreaming
$ spark-submit \
        --class com.yourtion.bigdata.spark.project.WebStatStreamingApp \
        --master local[4] \
        --name WebStatStreamingApp \
        --jars $(echo /opt/hbase/lib/*.jar | tr ' ' ',') \
        --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 \
        spark-streaming-train-1.0.jar \
        localhost:9092 streaming-project

// 查看数据
$ hbase shell
> scan 'imooc_course_click_count'
> scan 'imooc_course_search_count'
```

## TODO

- [ ] 完成其他图表
- [ ] 优化访问体验
- [ ] 添加自动刷新功能
- [ ] 使用 Redis 替代 HBase
- [ ] 替换废弃的方法与功能