package cn.xiaoguangbiao.edu.analysis.streaming

import cn.xiaoguangbiao.edu.bean.Answer
import cn.xiaoguangbiao.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.{SparkContext, streaming}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Author xiaoguangbiao
 * Desc
 * 从Kafka消费消息(消息中有用户id),
 * 然后从Redis中获取推荐模型的路径,并从路径中加载推荐模型ALSModel
 * 然后使用该模型给用户推荐易错题
 */
object StreamingRecommend {
  def main(args: Array[String]): Unit = {
    //TODO 0.准备环境
    val spark: SparkSession = SparkSession.builder().appName("StreamingAnalysis").master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") //本次测试时将分区数设置小一点,实际开发中可以根据集群规模调整大小,默认200
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, streaming.Seconds(5))
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //TODO 1.加载数据
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092", //kafka集群地址
      "key.deserializer" -> classOf[StringDeserializer], //key的反序列化规则
      "value.deserializer" -> classOf[StringDeserializer], //value的反序列化规则
      "group.id" -> "StreamingRecommend", //消费者组名称
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" -> "1000", //自动提交的时间间隔
      "enable.auto.commit" -> (true: java.lang.Boolean) //是否自动提交
    )
    val topics = Array("edu") //要订阅的主题
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //TODO 2.处理数据
    val valueDStream: DStream[String] = kafkaDStream.map(record => {
      record.value()
    })
    //{"student_id":"学生ID_47","textbook_id":"教材ID_1","grade_id":"年级ID_3","subject_id":"科目ID_3_英语","chapter_id":"章节ID_chapter_3","question_id":"题目ID_534","score":7,"answer_time":"2021-01-09 15:29:50","ts":"Jan 9, 2021 3:29:50 PM"}
    valueDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {

        //该rdd表示每个微批的数据
        //==1.获取path并加载模型
        //获取redis连接
        val jedis: Jedis = RedisUtil.pool.getResource
        //加载模型路径
        // jedis.hset("als_model", "recommended_question_id", path)
        val path: String = jedis.hget("als_model", "recommended_question_id")
        //根据路径加载模型
        val model: ALSModel = ALSModel.load(path)

        //==2.取出用户id
        val answerDF: DataFrame = rdd.coalesce(1).map(josnStr => {
          val gson = new Gson()
          gson.fromJson(josnStr, classOf[Answer])
        }).toDF
        //将用户id转为数字,因为后续模型推荐的时候需要数字格式的id
        val id2int = udf((student_id: String) => {
          student_id.split("_")(1).toInt
        })
        val studentIdDF: DataFrame = answerDF.select(id2int('student_id) as "student_id")

        //==3.使用模型给用户推荐题目
        val recommendDF: DataFrame = model.recommendForUserSubset(studentIdDF, 10)
        recommendDF.printSchema()
        /*
        root
       |-- student_id: integer (nullable = false) --用户id
       |-- recommendations: array (nullable = true)--推荐列表
       |    |-- element: struct (containsNull = true)
       |    |    |-- question_id: integer (nullable = true)--题目id
       |    |    |-- rating: float (nullable = true)--评分/推荐指数
         */
        recommendDF.show(false)
        /*
     +----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |student_id|recommendations                                                                                                                                                                             |
    +----------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |12        |[[1707, 2.900552], [641, 2.8934805], [815, 2.8934805], [1583, 2.8934805], [1585, 2.8774242], [1369, 2.8033295], [906, 2.772558], [2129, 2.668791], [1661, 2.585957], [1978, 2.5290453]]     |
    |14        |[[1627, 2.8925943], [446, 2.8925943], [1951, 2.8925943], [1412, 2.8925943], [1812, 2.8925943], [1061, 2.8816805], [1661, 2.874632], [1453, 2.8682063], [1111, 2.8643343], [1797, 2.7966104]]|
         */
        //处理推荐结果:取出用户id和题目id拼成字符串:"id1,id2,id3..."
        val recommendResultDF: DataFrame = recommendDF.as[(Int, Array[(Int, Float)])].map(t => {
          //val studentId: Int = t._1
          //val studentIdStr: String = "学生ID_"+ studentId
          //val questionIdsAndRating: Array[(Int, Float)] = t._2
          //val questionIds: Array[Int] = questionIdsAndRating.map(_._1)
          //val questionIdsStr: String = questionIds.mkString(",")
          val studentIdStr: String = "学生ID_" + t._1
          val questionIdsStr: String = t._2.map("题目ID_" + _._1).mkString(",")
          (studentIdStr, questionIdsStr)
        }).toDF("student_id", "recommendations")

        //将answerDF和recommendResultDF进行join
        val allInfoDF: DataFrame = answerDF.join(recommendResultDF, "student_id")

        //==4.输出结果到MySQL/HBase
        if (allInfoDF.count() > 0) {
          val properties = new java.util.Properties()
          properties.setProperty("user", "root")
          properties.setProperty("password", "root")
          allInfoDF
            .write
            .mode(SaveMode.Append)
            .jdbc("jdbc:mysql://localhost:3306/edu?useUnicode=true&characterEncoding=utf8", "t_recommended", properties)
        }

        //关闭redis连接
        jedis.close()
      }
    }
    )

    //TODO 3.输出结果

    //TODO 4.启动并等待停止
    ssc.start()
    ssc.awaitTermination()

    //TODO 5.关闭资源
    ssc.stop(stopSparkContext = true, stopGracefully = true) //优雅关闭
  }

}
// Am*nX Bn*k = Cm*k  和矩阵分解