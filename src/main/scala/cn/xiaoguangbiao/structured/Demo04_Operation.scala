package cn.xiaoguangbiao.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming的Operation
 */
object Demo04_Operation {
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
    /*
    root
     |-- value: string (nullable = true)
     */
    //df.show()// Queries with streaming sources must be executed with writeStream.start();

    //TODO 2.处理数据
    //TODO ====DSL
    val ds: Dataset[String] = df.as[String]
    val wordsDS: Dataset[String] = ds.flatMap(_.split(" "))
    val result1: Dataset[Row] = wordsDS
      .groupBy('value)
      .count()
      .orderBy('count.desc)


    //TODO ====SQL
    wordsDS.createOrReplaceTempView("t_words")
    val sql:String =
      """
        |select value,count(*) as counts
        |from t_words
        |group by value
        |order by counts desc
        |""".stripMargin
    val result2: DataFrame = spark.sql(sql)

    //TODO 3.输出结果
    result1.writeStream
        .format("console")
        .outputMode("complete")
    //TODO 4.启动
        .start()
        //.awaitTermination()//注意:后面还有代码要执行,所以这里需要注释掉

    result2.writeStream
      .format("console")
      .outputMode("complete")
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
