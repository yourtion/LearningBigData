

## 执行

### SparkApp

```bash
${SPARK_HOME}/bin/spark-submit \
    --class com.yourtion.bigdata.c08.SparkApp \
    --master local \
    --jars kudu-client-1.7.0.jar,kudu-spark2_2.11-1.7.0.jar \
    --conf spark.time=20181007 \
    --conf spark.raw.path="hdfs://yhost:8020/pk/access/20181007" \
    --conf spark.ip.path="hdfs://yhost:8020/pk/ip.txt" \
    sparksql-train-1.0.jar
```