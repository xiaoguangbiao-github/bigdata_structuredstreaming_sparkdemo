package cn.xiaoguangbiao.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming的Sink_Location
 */
object Demo06_Sink_Location {
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
    val result: Dataset[Row] = ds.flatMap(_.split(" "))
      .groupBy('value)
      .count()
      .orderBy('count.desc)

    //TODO 3.输出结果
    /*result.writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()*/

    val query: StreamingQuery = result.writeStream
      .format("memory")
      .queryName("t_result")
      .outputMode("complete")
      //TODO 4.启动并等待结束
      .start()
      //.awaitTermination()
    while(true){
      spark.sql("select * from t_result").show
      Thread.sleep(3000)
    }

    //query.awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
