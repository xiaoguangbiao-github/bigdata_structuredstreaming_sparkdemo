package cn.xiaoguangbiao.edu.model

import cn.xiaoguangbiao.edu.bean.{Answer, Rating}
import cn.xiaoguangbiao.edu.utils.RedisUtil
import com.google.gson.Gson
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 训练推荐模型
  */
object ALSModeling {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ALSModeling")
      .config("spark.local.dir", "temp")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    //2.加载数据并转换为:Dataset[Rating(学生id,问题id,推荐指数)]
    val path = "data/output/question_info.json"
    val answerInfoDF: Dataset[Rating] = spark.sparkContext.textFile(path)
      .map(parseAnswerInfo)
      .toDS()
      .cache()

    //3.划分数据集Array(80%训练集, 20%测试集)
    val randomSplits: Array[Dataset[Rating]] = answerInfoDF.randomSplit(Array(0.8, 0.2), 11L)

    //4.构建ALS模
    val als: ALS = new ALS()
      .setRank(20)//隐藏因子
      .setMaxIter(15)//迭代次数
      .setRegParam(0.09)//正则化参数
      .setUserCol("student_id")
      .setItemCol("question_id")
      .setRatingCol("rating")

    //5.使用训练集进行训练
    val model: ALSModel = als.fit(randomSplits(0).cache()).setColdStartStrategy("drop")

    //6.获得推荐
    val recommend: DataFrame = model.recommendForAllUsers(20)

    //7.对测试集进行预测
    val predictions: DataFrame = model.transform(randomSplits(1).cache())

    //8.使用RMSE(均方根误差)评估模型误差
    val evaluator: RegressionEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")//均方根误差
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse: Double = evaluator.evaluate(predictions)//均方根误差

    //9.输出结果
    //显示训练集数据
    randomSplits(0).foreach(x =>println("训练集： " + x))
    //显示测试集数据
    randomSplits(1).foreach(x => println("测试集： " + x))
    //推荐结果
    recommend.foreach(x => println("学生ID：" + x(0) + " ,推荐题目 " + x(1)))
    //打印预测结果
    predictions.foreach(x => println("预测结果:  " + x))
    //输出误差
    println("模型误差评估："  + rmse)

    //10.将训练好的模型保存到文件系统并将文件系统的路径存储到Redis
    val jedis = RedisUtil.pool.getResource
    //jedis.select(1)
    if(rmse <= 1.5){
      val path = "data/output/als_model/" + System.currentTimeMillis()
      model.save(path)
      jedis.hset("als_model", "recommended_question_id", path)
      println("模型path信息已保存到redis")
    }

    //11.释放缓存/关闭资源
    answerInfoDF.unpersist()
    randomSplits(0).unpersist()
    randomSplits(1).unpersist()
    RedisUtil.pool.returnResource(jedis)
  }

  /**
   * 将学生答题的详细信息转为Rating(学生id,问题id,推荐指数)
   */
  def parseAnswerInfo(json: String): Rating = {
    //1.获取学生答题信息(学生id,题目id,题目得分)
    val gson: Gson = new Gson()
    val answer: Answer = gson.fromJson(json, classOf[Answer])
    val studentID: Long = answer.student_id.split("_")(1).toLong
    val questionID: Long = answer.question_id.split("_")(1).toLong
    val rating: Int = answer.score

    //2.计算推荐指数:得分低的题目,推荐指数高
    val ratingFix: Int =
      if(rating <= 3) 3
      else if(rating > 3 && rating <= 8) 2
      else 1

    //3.返回学生id,问题id,推荐指数
    Rating(studentID, questionID, ratingFix)
  }
}