package cn.xiaoguangbiao.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 演示StructuredStreaming的Source-File
 */
object Demo03_Source_File {
  def main(args: Array[String]): Unit = {
    //TODO 0.创建环境
    //因为StructuredStreaming基于SparkSQL的且编程API/数据抽象是DataFrame/DataSet,所以这里创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().appName("sparksql").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val csvSchema: StructType = new StructType()
      .add("name", StringType, nullable = true)
      .add("age", IntegerType, nullable = true)
      .add("hobby", StringType, nullable = true)


    //TODO 1.加载数据
    val df: DataFrame = spark.readStream
      .option("sep", ";")
      .option("header", "false")
      .schema(csvSchema)//注意:流式处理对于结构化数据哪怕是有约束也需要单独指定
      .format("csv").load("data/input/persons") //.csv("data/input/persons")

    //TODO 2.处理数据


    //TODO 3.输出结果
    df.writeStream
        .format("console")
        //Complete output mode not supported when there are no streaming aggregations
        //.outputMode("complete")
        .outputMode("append")
        .option("truncate",false)//表示对列不进行截断,也就是对列内容全部展示
    //TODO 4.启动并等待结束
        .start()
        .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
