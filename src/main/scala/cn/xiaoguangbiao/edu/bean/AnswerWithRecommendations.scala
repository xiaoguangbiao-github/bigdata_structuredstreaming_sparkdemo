package cn.xiaoguangbiao.edu.bean

import java.sql.Timestamp

/**
 * 学生答题信息和推荐的题目ids样例类
 */
case class AnswerWithRecommendations(
            student_id: String,//学生ID
            textbook_id: String,//教材ID
            grade_id: String,//年级ID
            subject_id: String,//科目ID
            chapter_id: String,//章节ID
            question_id: String,//题目ID
            score: Int,//题目得分，0~10分
            answer_time: String,//答题提交时间，yyyy-MM-dd HH:mm:ss字符串形式
            ts: Timestamp,//答题提交时间，时间戳形式
            recommendations: String //推荐的题目ids
          ) extends Serializable