package cn.xiaoguangbiao.structured

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming
 */
object Demo12_Deduplication {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //TODO 1.加载数据
    val socketDF: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    //TODO 2.处理数据:添加schema
    //{"eventTime": "2016-01-10 10:01:50","eventType": "browse","userID":"1"}
    //{"eventTime": "2016-01-10 10:01:50","eventType": "click","userID":"1"}
    val schemaDF: DataFrame = socketDF
      .as[String]
      .filter(StringUtils.isNotBlank(_))
      .select(
        get_json_object($"value", "$.eventTime").as("eventTime"),
        get_json_object($"value", "$.eventType").as("eventType"),
        get_json_object($"value", "$.userID").as("userID")
      )

    //TODO 3.数据处理
    //对网站用户日志数据，按照userId和eventTime、eventType去重统计
    val result: Dataset[Row] = schemaDF
      .dropDuplicates("userID","eventTime","eventType")
      .groupBy("userID")
      .count()


    result.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}

//0.kafka准备好
//1.启动数据模拟程序
//2.启动Demo10_Kafka_IOT
