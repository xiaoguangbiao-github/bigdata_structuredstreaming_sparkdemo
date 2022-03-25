package cn.xiaoguangbiao.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming的Source-Socket
 */
object Demo08_Sink_Trigger_Checkpoint {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    //TODO 1.加载数据
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node1")
      .option("port", 9999)
      .load()

    df.printSchema()

    //TODO 2.处理数据
    val ds: Dataset[String] = df.as[String]
    val result: Dataset[Row] = ds.coalesce(1).flatMap(_.split(" "))
      .groupBy('value)
      .count()
      //.orderBy('count.desc)

    //TODO 3.输出结果
    result.writeStream
        .format("console")
        .outputMode("complete")
        //触发间隔:
        //1.默认的不写就是:尽可能快的运行微批,Default trigger (runs micro-batch as soon as it can)
        //2.指定0也是尽可能快的运行
        // .trigger(Trigger.ProcessingTime("0 seconds"))
        //3.指定时间间隔
        //.trigger(Trigger.ProcessingTime("5 seconds"))
        //4.触发1次
        //.trigger(Trigger.Once())
        //5.连续处理并指定Checkpoint时间间隔,实验的
        .trigger(Trigger.Continuous("1 second"))
        .option("checkpointLocation", "./ckp"+System.currentTimeMillis())
    //TODO 4.启动并等待结束
        .start()
        .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
