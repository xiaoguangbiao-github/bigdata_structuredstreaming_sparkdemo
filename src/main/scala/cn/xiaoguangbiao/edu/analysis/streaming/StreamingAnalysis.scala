package cn.xiaoguangbiao.edu.analysis.streaming

import cn.xiaoguangbiao.edu.bean.Answer
import com.google.gson.Gson
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Author xiaoguangbiao
 * Desc 实时的从Kafka的edu主题消费数据,并做实时的统计分析,结果可以直接输出到控制台或mysql
 */
object StreamingAnalysis {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("StreamingAnalysis").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")//本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO 1.加载数据
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092")
      .option("subscribe", "edu")
      .load()
    val valueDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    //{"student_id":"学生ID_31","textbook_id":"教材ID_1","grade_id":"年级ID_6","subject_id":"科目ID_2_语文","chapter_id":"章节ID_chapter_3","question_id":"题目ID_1003","score":7,"answer_time":"2021-01-09 14:53:28","ts":"Jan 9, 2021 2:53:28 PM"}

    //TODO 2.处理数据
    //---数据预处理
    //解析json-方式1:
    /*valueDS.select(
      get_json_object($"value", "$.student_id").as("student_id"),
      //.....
    )*/
    //解析json-方式2:将每一条json字符串解析为一个样例类对象
    val answerDS: Dataset[Answer] = valueDS.map(josnStr => {
      val gson = new Gson()
      //json--->对象
      gson.fromJson(josnStr, classOf[Answer])
    })
   //---实时分析
    //TODO ==实时分析需求1:统计top10热点题
    //SQL
    /*
    val result1 = spark.sql(
    """SELECT
      |  question_id, COUNT(1) AS frequency
      |FROM
      |  t_answer
      |GROUP BY
      |  question_id
      |ORDER BY
      |  frequency
      |DESC
      |LIMIT 10
    """.stripMargin)
     */
    //DSL
    val result1: Dataset[Row] = answerDS.groupBy('question_id)
      //.agg(count('question_id) as "count")
      .count()
      .orderBy('count.desc)
      .limit(10)

    //TODO ==实时分析需求2:统计top10答题活跃年级
    /*
    val result2 = spark.sql(
      """SELECT
        |  grade_id, COUNT(1) AS frequency
        |FROM
        |  t_answer
        |GROUP BY
        |  grade_id
        |ORDER BY
        |  frequency
        |DESC
    |LIMIT 10
      """.stripMargin)
     */
    val result2: Dataset[Row] = answerDS.groupBy('grade_id)
      .count()
      .orderBy('count.desc)
      .limit(10)

    //TODO ==实时分析需求3:统计top10热点题并带上所属科目
    /*
    注意:select...group语句下,select 后面的字段要么是分组字段,要么是聚合字段
    val result1 = spark.sql(
    """SELECT
      |  question_id,first(subject_id), COUNT(1) AS frequency
      |FROM
      |  t_answer
      |GROUP BY
      |  question_id
      |ORDER BY
      |  frequency
      |DESC
      |LIMIT 10
    """.stripMargin)
     */
    val result3: Dataset[Row] = answerDS.groupBy('question_id)
      .agg(
        first('subject_id) as "subject_id",
        count('question_id) as "count"
      )
      .orderBy('count.desc)
      .limit(10)

    //TODO ==实时分析需求4:统计每个学生的得分最低的题目top10并带上是所属哪道题
    /*
    val result4 = spark.sql(
      """SELECT
        |  student_id, FIRST(question_id), MIN(score)
        |FROM
        |  t_answer
        |GROUP BY
        |  student_id
    |order by
    |  score
    |limit 10
      """.stripMargin)
     */
    val result4: Dataset[Row] = answerDS.groupBy('student_id)
      .agg(
        min('score) as "minscore",
        first('question_id)
      )
      .orderBy('minscore)
      .limit(10)


    //TODO 3.输出结果
    result1.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    result2.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    result3.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    result4.writeStream
      .format("console")
      .outputMode("complete")
      //TODO 4.启动并等待结束
      .start()
      .awaitTermination()

    //TODO 5.关闭资源
    spark.stop()
  }
}
