package cn.xiaoguangbiao.structured

import java.sql.Timestamp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming
 * 基于事件时间的窗口计算+水位线/水印解决数据延迟到达(能够容忍一定程度上的延迟,迟到严重的还是会被丢弃)
 * 每隔5s计算最近10s的数据,withWatermark设置为10s
 *
 * 2019-10-10 12:00:07,dog
 * 2019-10-10 12:00:08,owl
 *
 * 2019-10-10 12:00:14,dog
 * 2019-10-10 12:00:09,cat
 *
 * 2019-10-10 12:00:15,cat
 * 2019-10-10 12:00:08,dog     --迟到不严重,会被计算,影响最后的统计结果
 * 2019-10-10 12:00:13,owl
 * 2019-10-10 12:00:21,owl
 *
 * 2019-10-10 12:00:04,donkey  --迟到严重,不会被计算,不影响最后的统计结果
 * 2019-10-10 12:00:17,owl     --影响结果
 */
object Demo11_Eventtime_Window_Watermark {
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
    val wordDF = socketDF
      .as[String]
      .filter(StringUtils.isNotBlank(_))
      // 将每行数据进行分割: 2019-10-12 09:00:02,cat
      .map(line => {
        val arr = line.trim.split(",")
        val timestampStr: String = arr(0)
        val word: String = arr(1)
        (Timestamp.valueOf(timestampStr), word)
      })
      // 设置列的名称
      .toDF("timestamp", "word")
    //需求:每隔5s计算最近10s的数据,withWatermark设置为10s
    val resultDF = wordDF
      //withWatermark(指定事件时间是哪一列,指定时间阈值)
      .withWatermark("timestamp", "10 seconds")
      .groupBy(
        //指定基于事件时间做窗口聚合计算:WordCount
        //window(指定事件时间是哪一列,窗口长度,滑动间隔)
        window($"timestamp", "10 seconds", "5 seconds"),
        $"word")
      .count()


    //TODO 3.输出结果-控制台
    resultDF.writeStream
      .outputMode(OutputMode.Update()) //为了方便观察只输出有变化的数据
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()

    spark.stop()

    //TODO 5.关闭资源
    spark.stop()
  }
}

//0.kafka准备好
//1.启动数据模拟程序
//2.启动Demo10_Kafka_IOT
