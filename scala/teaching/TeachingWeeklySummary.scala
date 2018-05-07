package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/10/29.
  */
object TeachingWeeklySummary {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("HiveAnswerSpark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles","true")
      .set("spark.speculation","true")
      .set("spark.task.maxFailures","8")
      .set("spark.akka.timeout","300")
      .set("spark.network.timeout","300")
      .set("spark.yarn.max.executor.failures","100")
    val sc_1 = new SparkContext(sparkConf)
    val hc_1 = new HiveContext(sc_1)
    import hc_1.sql
    /*val DATE6AGO = args(0).toString
    val DATENOW = args(1).toString
    val DAY6AGO = args(2).toString
    val DAY = args(3).toString
    val DATE = args(4).toString*/
    /*班级学生人数*/
    val str_drop_class_num=""" drop table test.class_student_num """.stripMargin
    sql(str_drop_class_num)
    val str_create_class_num=""" create  table  test.class_student_num as
                        |select class_id,count(distinct system_id) num
                        |from test.user_type_num where type=2 group by class_id """.stripMargin
    sql(str_create_class_num)
    /*学科下各班级答题*/
    val str_drop_class_answer=""" drop table test.class_answer_num """.stripMargin
    sql(str_drop_class_answer)
    val str_create_class_answer=""" create table  test.class_answer_num as
                                  |select subject_name,class_id, count(question_id) ques_num
                                  |from test.student_question where exercise_source in (1,6,7,8) group by subject_name,class_id """.stripMargin
    sql(str_create_class_answer)
     /*班级活跃汇总*/
    val str_drop_cds=""" drop table test.class_daily_summary """.stripMargin
    sql(str_drop_cds)
    val str_create_cds=""" create  table  test.class_daily_summary as
                         |			select org_id,org_type,0 grade_id, 0 class_id,9999 pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type
                         |			union all
                         |			select org_id,org_type,0 grade_id, 0 class_id,pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type,pad_class
                         |			union all
                         |			select org_id,org_type,grade_id, 0 class_id,9999 pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type,grade_id
                         |			union all
                         |			select org_id,org_type,grade_id, 0 class_id,pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type,grade_id,pad_class
                         |			union all
                         |			select org_id,org_type,grade_id,class_id, 9999 pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type,grade_id,class_id
                         |			union all
                         |			select org_id,org_type,grade_id,class_id, pad_class,count(distinct class_id) class_daily_num,time
                         |			from  test.class_daily where class_daily=1
                         |			group by time,org_id,org_type,grade_id,class_id,pad_class """.stripMargin
    sql(str_create_cds)
    /*APP用户汇总*/
    val str_drop_saus=""" drop table test.student_app_user_summary """.stripMargin
    sql(str_drop_saus)
    val str_create_saus=""" create table test.student_app_user_summary as
                          |select org_id,org_type,0 grade_id,9999 pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |time
                          |from test.student_app_user
                          |group by time,org_id,org_type
                          |union all
                          |select org_id,org_type,0 grade_id,pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |time
                          |from test.student_app_user
                          |group by time,org_id,org_type,pad_class
                          |union all
                          |select org_id,org_type,grade_id,9999 pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |time
                          |from test.student_app_user
                          |group by time,org_id,org_type,grade_id
                          |union all
                          |select org_id,org_type,grade_id,pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |time
                          |from test.student_app_user
                          |group by time,org_id,org_type,grade_id,pad_class
                          |----七日汇总
                          |union all
                          |select org_id,org_type,0 grade_id,9999 pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |'9999-12-31' time
                          |from test.student_app_user
                          |group by org_id,org_type
                          |union all
                          |select org_id,org_type,0 grade_id,pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |'9999-12-31' time
                          |from test.student_app_user
                          |group by org_id,org_type,pad_class
                          |union all
                          |select org_id,org_type,grade_id,9999 pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |'9999-12-31' time
                          |from test.student_app_user
                          |group by org_id,org_type,grade_id
                          |union all
                          |select org_id,org_type,grade_id,pad_class,
                          |count(distinct case when type=1 then system_id end) wk_app_num,
                          |count(distinct case when type=2 then system_id end) xt_app_num,
                          |'9999-12-31' time
                          |from test.student_app_user
                          |group by org_id,org_type,grade_id,pad_class """.stripMargin
    sql(str_create_saus)
    /*学生活跃汇总*/
    val str_drop_sds=""" drop table test.student_daily_summary """.stripMargin
    sql(str_drop_sds)
    val str_create_sds=""" create table test.student_daily_summary as
                         |		select org_id,org_type,0 grade_id,0 class_id,9999 pad_class,count(distinct system_id) studnet_daily_num,time from test.student_daily
                         |		group by time,org_id,org_type
                         |		union all
                         |		select org_id,org_type,0 grade_id,0 class_id,pad_class,count(distinct system_id) studnet_daily_num,time from test.student_daily
                         |		group by time,org_id,org_type,pad_class
                         |		union all
                         |		select org_id,org_type,grade_id,0 class_id,9999 pad_class,count(distinct system_id) studnet_daily_num,time from test.student_daily
                         |		group by time,org_id,org_type,grade_id
                         |		union all
                         |		select org_id,org_type,grade_id,0 class_id,pad_class,count(distinct system_id) studnet_daily_num,time from test.student_daily
                         |		group by time,org_id,org_type,grade_id,pad_class
                         |		union all
                         |		select org_id,org_type,grade_id,class_id,9999 pad_class,count(distinct system_id) studnet_daily_num,time from test.student_daily
                         |		group by time,org_id,org_type,grade_id,class_id
                         |		union all
                         |		select org_id,org_type,grade_id,class_id,pad_class,count(distinct system_id) studnet_daily_num ,time from test.student_daily
                         |		group by time,org_id,org_type,grade_id,class_id,pad_class """.stripMargin
    sql(str_create_sds)
    /*学生答题汇总*/
    val str_drop_sqs=""" drop table test.student_question_summary """.stripMargin
    sql(str_drop_sqs)
    val str_create_sqs=""" create table test.student_question_summary as
                         |      select org_id,org_type,0 grade_id,0 class_id,9999 pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in (1,6,7,8)
                         |      group by time,org_id,org_type
                         |			union all
                         |			select org_id,org_type,0 grade_id,0 class_id,pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in (1,6,7,8)
                         |      group by time,org_id,org_type,pad_class
                         |			union all
                         |			select org_id,org_type,grade_id,0 class_id,9999 pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in  (1,6,7,8)
                         |      group by time,org_id,org_type,grade_id
                         |			union all
                         |			select org_id,org_type,grade_id,0 class_id,pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in (1,6,7,8)
                         |      group by time,org_id,org_type,grade_id,pad_class
                         |			union all
                         |			select org_id,org_type,grade_id,class_id,9999 pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in (1,6,7,8)
                         |      group by time,org_id,org_type,grade_id,class_id
                         |			union all
                         |			select org_id,org_type,grade_id,class_id,pad_class,count(question_id) answer_num,time from test.student_question
                         |			where exercise_source in (1,6,7,8)
                         |      group by time,org_id,org_type,grade_id,class_id,pad_class """.stripMargin
    sql(str_create_sqs)
    /*学生各场景答题汇总*/
    val str_drop_sss=""" drop table   test.student_scence_summary """.stripMargin
    sql(str_drop_sss)
    val str_create_sss=""" create table test.student_scence_summary as
                         |select h.org_id,h.org_type,h.grade_id,h.pad_class,
                         | h.xiti_user_num,h.xta_num,h.xtr_num,(h.xtr_num/h.xta_num) xtl,
                         | h.zya_num,h.zyr_num,(h.zyr_num/h.zya_num) zyl,
                         | h.cpa_num,h.cpr_num,(h.cpr_num/h.cpa_num) cpl,
                         | h.fxa_num,h.fxr_num,(h.fxr_num/h.fxa_num) fxl,
                         | h.yxa_num,h.yxr_num,(h.yxr_num/h.yxa_num) yxl,
                         | h.ska_num,h.skr_num,(h.skr_num/h.ska_num) skl,h.time
                         |from
                         |(select org_id,org_type,0 grade_id,9999 pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |          when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |time
                         |from test.student_question
                         |group by time,org_id,org_type
                         |union all
                         |select org_id,org_type,0 grade_id,pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |          when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |time
                         |from test.student_question
                         |group by time,org_id,org_type,pad_class
                         |union all
                         |select org_id,org_type,grade_id,9999 pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |time
                         |from test.student_question
                         |group by time,org_id,org_type,grade_id
                         |union all
                         |select org_id,org_type,grade_id, pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |time
                         |from test.student_question
                         |group by time,org_id,org_type,grade_id,pad_class
                         |----七日汇总数据
                         |union all
                         |select org_id,org_type,0 grade_id,9999 pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |'9999-12-31' time
                         |from test.student_question
                         |group by org_id,org_type
                         |union all
                         |select org_id,org_type,0 grade_id,pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |'9999-12-31' time
                         |from test.student_question
                         |group by org_id,org_type,pad_class
                         |union all
                         |select org_id,org_type,grade_id,9999 pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |'9999-12-31' time
                         |from test.student_question
                         |group by org_id,org_type,grade_id
                         |union all
                         |select org_id,org_type,grade_id, pad_class,
                         |--习题(exercise_source=9 10)
                         |count(distinct case when exercise_source in (9,10) then student_id end) xiti_user_num,
                         |count(case when exercise_source in (9,10)  then question_id   end) xta_num,
                         |sum(case when exercise_source in (9,10) and ret_num=1 then 1
                         |         when exercise_source in (9,10) and ret_num=5 then 0.5 end
                         |) xtr_num,
                         |--作业(exercise_source=6)
                         |count(case when exercise_source=6  then question_id   end) zya_num,
                         |sum(case when exercise_source=6 and ret_num=1 then 1
                         |         when exercise_source=6 and ret_num=5 then 0.5 end) zyr_num,
                         |--测评(exercise_source=7)
                         |count(case when exercise_source=7  then question_id   end) cpa_num,
                         |sum(case when exercise_source=7 and ret_num=1 then 1
                         |         when exercise_source=7 and ret_num=5 then 0.5 end) cpr_num,
                         |--复习(exercise_source=3,4,5)
                         |count(case when exercise_source in(3,4,5)  then question_id   end) fxa_num,
                         |sum(case when exercise_source in(3,4,5) and ret_num=1 then 1
                         |         when exercise_source in(3,4,5) and ret_num=5 then 0.5 end) fxr_num,
                         |--预习(exercise_source=8)
                         |count(case when exercise_source=8  then question_id   end) yxa_num,
                         |sum(case when exercise_source=8 and ret_num=1 then 1
                         |         when exercise_source=8 and ret_num=5 then 0.5 end) yxr_num,
                         |--上课(exercise_source=1)
                         |count(case when exercise_source=1  then question_id   end) ska_num,
                         |sum(case when exercise_source=1 and ret_num=1 then 1
                         |         when exercise_source=1 and ret_num=5 then 0.5 end) skr_num,
                         |'9999-12-31' time
                         |from test.student_question
                         |group by org_id,org_type,grade_id,pad_class
                         |) h """.stripMargin
    sql(str_create_sss)
    /*学生各学科答题汇总*/
    val str_drop_ssas=""" drop table  test.student_subject_answer_summary """.stripMargin
    sql(str_drop_ssas)
    val str_create_ssas="""  create  table test.student_subject_answer_summary as
                          |			select utn.org_id org_id,utn.org_type org_type,0 grade_id,9999 pad_class,
                          |					utn.subject_name subject_name,utn.system_id system_id,
                          |				    sum(uts.num) student_num,
                          |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                          |					count(distinct utn.class_id) manage_class_num
                          |					from (select * from test.user_type_num where type=1) utn
                          |					join test.class_student_num uts on uts.class_id=utn.class_id
                          |					left join test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                          |			group by  utn.org_id,utn.org_type,utn.subject_name,utn.system_id
                          |
                          |			union all
                          |			select utn.org_id org_id,utn.org_type org_type,0 grade_id,utn.pad_class pad_class,
                          |					utn.subject_name subject_name,utn.system_id system_id,
                          |					sum(uts.num) student_num,
                          |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                          |					count(distinct utn.class_id) manage_class_num
                          |					from (select * from test.user_type_num where type=1) utn
                          |					join test.class_student_num  uts on uts.class_id=utn.class_id
                          |					left join test.class_answer_num  sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                          |			group by  utn.org_id,utn.org_type,utn.pad_class,utn.subject_name,utn.system_id
                          |			union all
                          |			select utn.org_id org_id,utn.org_type org_type,utn.grade_id grade_id,9999 pad_class,
                          |					utn.subject_name subject_name,utn.system_id system_id,
                          |					sum(uts.num) student_num,
                          |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                          |					count(distinct utn.class_id) manage_class_num
                          |					from (select * from test.user_type_num where type=1) utn
                          |					join test.class_student_num  uts on uts.class_id=utn.class_id
                          |					left join test.class_answer_num  sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                          |			group by  utn.org_id,utn.org_type,utn.grade_id,utn.subject_name,utn.system_id
                          |			union all
                          |			select utn.org_id org_id,utn.org_type org_type,utn.grade_id grade_id,utn.pad_class pad_class,
                          |			        utn.subject_name subject_name,utn.system_id system_id,
                          |					sum(uts.num) student_num,
                          |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                          |					count(distinct utn.class_id) manage_class_num
                          |					from (select * from test.user_type_num where type=1) utn
                          |					join test.class_student_num  uts on uts.class_id=utn.class_id
                          |					left join  test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                          |			group by  utn.org_id,utn.org_type,utn.grade_id,utn.pad_class,utn.subject_name,utn.system_id  """.stripMargin
    sql(str_create_ssas)
    /*学生各学科各场景答题汇总*/
    val str_drop_ssss=""" drop table test.student_subject_scence_summary """.stripMargin
    sql(str_drop_ssss)
    val str_create_ssss="""  create table test.student_subject_scence_summary as
                          |select org_id,org_type,subject_name,0 grade_id,9999 pad_class,
                          |count(distinct case when exercise_source=7 then student_id end) cp_user_num,
                          |count(case when exercise_source=7 then question_id end) cp_answer_num,
                          |
                          |count(distinct case when exercise_source=1 then student_id end) sk_user_num,
                          |count(case when exercise_source=1 then question_id end) sk_answer_num,
                          |
                          |count(distinct case when exercise_source=8 then student_id end) yx_user_num,
                          |count(case when exercise_source=8 then question_id end) yx_answer_num,
                          |
                          |count(distinct case when exercise_source=6 then student_id end) zy_user_num,
                          |count(case when exercise_source=6 then question_id end) zy_answer_num
                          |from test.student_question
                          |group by org_id,org_type,subject_name
                          |union all
                          |select org_id,org_type,subject_name,0 grade_id,pad_class,
                          |count(distinct case when exercise_source=7 then student_id end) cp_user_num,
                          |count(case when exercise_source=7 then question_id end) cp_answer_num,
                          |
                          |count(distinct case when exercise_source=1 then student_id end) sk_user_num,
                          |count(case when exercise_source=1 then question_id end) sk_answer_num,
                          |
                          |count(distinct case when exercise_source=8 then student_id end) yx_user_num,
                          |count(case when exercise_source=8 then question_id end) yx_answer_num,
                          |
                          |count(distinct case when exercise_source=6 then student_id end) zy_user_num,
                          |count(case when exercise_source=6 then question_id end) zy_answer_num
                          |from test.student_question
                          |group by org_id,org_type,subject_name,pad_class
                          |union all
                          |select org_id,org_type,subject_name,grade_id,9999 pad_class,
                          |count(distinct case when exercise_source=7 then student_id end) cp_user_num,
                          |count(case when exercise_source=7 then question_id end) cp_answer_num,
                          |
                          |count(distinct case when exercise_source=1 then student_id end) sk_user_num,
                          |count(case when exercise_source=1 then question_id end) sk_answer_num,
                          |
                          |count(distinct case when exercise_source=8 then student_id end) yx_user_num,
                          |count(case when exercise_source=8 then question_id end) yx_answer_num,
                          |
                          |count(distinct case when exercise_source=6 then student_id end) zy_user_num,
                          |count(case when exercise_source=6 then question_id end) zy_answer_num
                          |from test.student_question
                          |group by org_id,org_type,subject_name,grade_id
                          |union all
                          |select org_id,org_type,subject_name,grade_id,pad_class,
                          |count(distinct case when exercise_source=7 then student_id end) cp_user_num,
                          |count(case when exercise_source=7 then question_id end) cp_answer_num,
                          |
                          |count(distinct case when exercise_source=1 then student_id end) sk_user_num,
                          |count(case when exercise_source=1 then question_id end) sk_answer_num,
                          |
                          |count(distinct case when exercise_source=8 then student_id end) yx_user_num,
                          |count(case when exercise_source=8 then question_id end) yx_answer_num,
                          |
                          |count(distinct case when exercise_source=6 then student_id end) zy_user_num,
                          |count(case when exercise_source=6 then question_id end) zy_answer_num
                          |from test.student_question
                          |group by org_id,org_type,subject_name,grade_id,pad_class  """.stripMargin
    sql(str_create_ssss)
    /*学生答题(平均答题,管理班级数)汇总*/
    val str_drop_sas=""" drop table   test.subject_answer_summary  """.stripMargin
    sql(str_drop_sas)
    val str_create_sas=""" create table test.subject_answer_summary as
                         |select utn.org_id org_id,utn.org_type org_type,0 grade_id,9999 pad_class,
                         |					utn.subject_name subject_name,'无' uname,
                         |					sum(uts.num) student_num,
                         |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                         |					count(distinct utn.class_id) manage_class_num
                         |					from (select * from test.user_type_num where type=1) utn
                         |					join test.class_student_num uts on uts.class_id=utn.class_id
                         |					left join test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                         |			group by utn.org_id,utn.org_type,utn.subject_name
                         |			union all
                         |			select utn.org_id org_id,utn.org_type org_type,0 grade_id,utn.pad_class pad_class,
                         |					utn.subject_name subject_name,'无' uname,
                         |					sum(uts.num) student_num,
                         |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                         |					count(distinct utn.class_id) manage_class_num
                         |					from (select * from test.user_type_num where type=1) utn
                         |					join test.class_student_num  uts on uts.class_id=utn.class_id
                         |					left join test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                         |			group by utn.org_id,utn.org_type,utn.pad_class,utn.subject_name
                         |			union all
                         |			select utn.org_id org_id,utn.org_type org_type,utn.grade_id grade_id,9999 pad_class,
                         |					utn.subject_name subject_name,'无' uname,
                         |					sum(uts.num) student_num,
                         |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                         |					count(distinct utn.class_id) manage_class_num
                         |					from (select * from test.user_type_num where type=1) utn
                         |					join test.class_student_num  uts on uts.class_id=utn.class_id
                         |					left join test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                         |			group by utn.org_id,utn.org_type,utn.grade_id,utn.subject_name
                         |			union all
                         |			select utn.org_id org_id,utn.org_type org_type,utn.grade_id grade_id,utn.pad_class pad_class,
                         |					utn.subject_name subject_name,'无' uname,
                         |					sum(uts.num) student_num,
                         |					sum(case when sq.ques_num is not  null  then sq.ques_num else 0 end) subject_answer_num,
                         |					count(distinct utn.class_id) manage_class_num
                         |					from (select * from test.user_type_num where type=1) utn
                         |					join test.class_student_num uts on uts.class_id=utn.class_id
                         |					left join  test.class_answer_num sq on utn.class_id=sq.class_id and sq.subject_name=utn.subject_name
                         |			group by utn.org_id,utn.org_type,utn.grade_id,utn.pad_class,utn.subject_name """.stripMargin
    sql(str_create_sas)
    /*老师各场景下发布*/
    val  str_drop_tsps=""" drop  table  test.teacher_subject_publish_summary """.stripMargin
    sql(str_drop_tsps)
    val str_create_tsps=""" create table test.teacher_subject_publish_summary as
                         |			select org_id,org_type,system_id,subject_name,0 grade_id,9999 pad_class,
                         |						  sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |						  sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |						  sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |						  sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number
                         |						  from test.teacher_publish
                         |			group by org_id,org_type,subject_name,system_id
                         |			union all
                         |			select org_id,org_type,system_id,subject_name,0 grade_id,pad_class,
                         |						  sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |						  sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |						  sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |						  sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number
                         |						  from test.teacher_publish
                         |			group by org_id,org_type,pad_class,subject_name,system_id
                         |			union all
                         |			select org_id,org_type,system_id,subject_name,grade_id,9999 pad_class,
                         |						  sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |						  sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |						  sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |						  sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number
                         |						  from test.teacher_publish
                         |			group by org_id,org_type,grade_id,subject_name,system_id
                         |			union all
                         |			select org_id,org_type,system_id,subject_name,grade_id,pad_class,
                         |						  sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |						  sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |						  sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |						  sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number
                         |						  from test.teacher_publish
                         |			group by org_id,org_type,grade_id,pad_class,subject_name,system_id """.stripMargin
    sql(str_create_tsps)
    sc_1.stop()
  }
}
