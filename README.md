# FastApriori

FastApriori is an efficient and fast association rules recommendation algorithm on distributed data-parallel platforms. It optimizes the traditional Apriori algorithm and parallelizes it. In addition, FastApriori is built on the widely-used distributed data-parallel platform Apache Spark.

# Environment

- Apache Spark: Spark 2.10.4
- Apache HDFS: FastApriori uses HDFS as the distributed file system. The HDFS version is 2.6.5.
- Java: The JDK version is 1.8
- Scala: The Scala SDK version is 2.10.4

# Compile

FastApriori is built using Apache Maven.
To build FastApriori, Run `mvn scala:compile compile package` in the root directory.

# Run

The entry of FastApriori is defined in the scala class `com.hazzacheng.AR.Main`.

Run FastApriori with the following command:
```
    spark-submit \
        -v \
        --master [SPARK_MASTER_ADDRESS] \
        --name "FastApriori" \
        --class "com.hazzacheng.AR.Main" \
        --executor-cores 4 \
        --executor-memory 20G \
        --driver-memory 20G \
        AR.jar \
        <Input path> \
        <Output path> \
        <Temporary path> \
```

The run command contains the following parameters:

- `<Input path>`: The input data path on HDFS or local file system.
- `<Output path>`: The output data path on HDFS or local file system.
- `<Temporary path>`: The tempoaray data path on HDFS or local file system.