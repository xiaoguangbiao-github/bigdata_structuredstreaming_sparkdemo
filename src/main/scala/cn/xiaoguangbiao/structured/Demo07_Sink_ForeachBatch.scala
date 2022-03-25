package cn.xiaoguangbiao.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming的Sink_ForeachBatch
 */
object Demo07_Sink_ForeachBatch {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
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
    result.writeStream
      .foreachBatch((ds: Dataset[Row], batchId:Long) => {
        //自定义输出到控制台
        println("-------------")
        println(s"batchId:${batchId}")
        println("-------------")
        ds.show()
        //自定义输出到MySQL
        ds.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .format("jdbc")
          //.option("driver", "com.mysql.cj.jdbc.Driver")//MySQL-8
          //.option("url", "jdbc:mysql://localhost:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true")//MySQL-8
          .option("url", "jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8")
          .option("user", "root")
          .option("password", "root")
          .option("dbtable", "bigdata.t_struct_words")
          .save()
      })
      .outputMode("complete")
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()


    //TODO 5.关闭资源
    spark.stop()
  }
}
