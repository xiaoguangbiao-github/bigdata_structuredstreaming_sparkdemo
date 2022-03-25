package cn.xiaoguangbiao.edu.bean

/**
  * 学生题目推荐指数样例类
  */
case class Rating(
                   student_id: Long, //学生id
                   question_id: Long, //题目id
                   rating: Float //推荐指数
                 ) extends Serializable