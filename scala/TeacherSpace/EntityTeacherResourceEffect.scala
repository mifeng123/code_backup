package TeacherSpace

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object EntityTeacherResourceEffect {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("resource_socre")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.speculation", "true")
      .set("spark.task.maxFailures", "8")
      .set("spark.akka.timeout", "300")
      .set("spark.network.timeout", "300")
      .set("spark.yarn.max.executor.failures", "100")
    val sc_1 = new SparkContext(sparkConf)
    val hc_1 = new HiveContext(sc_1)
    import hc_1.sql

      val DAY = args(0).toString
      val DATE = args(1).toString
      /*val DATENOW = args(2).toString
      val DAY6AGO = args(3).toString*/
    /*知识点*/
    val  str_drop_tre=""" drop table test.topic_resource_effect """.stripMargin
    sql(str_drop_tre)
    val str_create_tre="""  create table test.topic_resource_effect as
                         |select 1 type,tmp.topic_id type_id,tmp.resource_type,tmp.resource_id,max(tmp.score) score from
                         |(
                         |--课件
                         |select  DISTINCT 1 resource_type,erp.creator_id  system_id,lrpr.resource_id,lrt.topic_id,etcs.score  from   neworiental_v3.entity_respackage_publish erp
                         |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                         |join neworiental_v3.entity_res_courseware erc on erc.id=lrpr.resource_id
                         |join neworiental_v3.link_resource_topic lrt on lrt.resource_id=erc.id and lrt.resource_type=0
                         |join neworiental_report.entity_teacher_topic_score etcs ON etcs.topic_id=lrt.topic_id and etcs.teacher_system_id=erp.creator_id
                         |where lrpr.resource_type=1 and erc.status=1
                         |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                         |union ALL
                         |--题集
                         |select  DISTINCT 2 resource_type, erp.creator_id  system_id,lrpr.resource_id,let.topic_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                         |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                         |join neworiental_v3.entity_exercise erc on erc.id=lrpr.resource_id
                         |join neworiental_v3.link_exercise_topic let on let.exercise_id=erc.id
                         |join neworiental_report.entity_teacher_topic_score etcs ON etcs.topic_id=let.topic_id and etcs.teacher_system_id=erp.creator_id
                         |where lrpr.resource_type=2 and erc.status=1
                         |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                         |union ALL
                         |--资料
                         |select  DISTINCT 3 resource_type,erp.creator_id  system_id,lrpr.resource_id,lrt.topic_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                         |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                         |join neworiental_v3.entity_res_clip erc on erc.id=lrpr.resource_id
                         |join neworiental_v3.link_resource_topic lrt on lrt.resource_id=erc.id and lrt.resource_type=1
                         |join neworiental_report.entity_teacher_topic_score etcs ON etcs.topic_id=lrt.topic_id and etcs.teacher_system_id=erp.creator_id
                         |where lrpr.resource_type=3 and erc.status=1
                         |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                         |union all
                         |--题目
                         |select  DISTINCT 4 resource_type, erp.creator_id  system_id,leq.question_id resource_id,lqt.topic_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                         |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                         |join neworiental_v3.entity_exercise erc on erc.id=lrpr.resource_id
                         |join neworiental_v3.link_exercise_question leq on leq.exercise_id=erc.id
                         |join neworiental_v3.link_question_topic lqt on lqt.question_id=leq.question_id
                         |join neworiental_report.entity_teacher_topic_score etcs ON etcs.topic_id=lqt.topic_id and etcs.teacher_system_id=erp.creator_id
                         |where lrpr.resource_type=2 and erc.status=1) tmp
                         |group by tmp.topic_id,tmp.resource_type,tmp.resource_id """.stripMargin
    sql(str_create_tre)

    /*章节*/
    val str_drop_cre=""" drop table test.chapter_resource_effect """.stripMargin
    sql(str_drop_cre)
    val  str_create_cre=""" create table test.chapter_resource_effect as
                          |select 2 type,tmp.chapter_id type_id,tmp.resource_type,tmp.resource_id,max(tmp.score) score from
                          |(
                          |--课件
                          |select  DISTINCT 1 resource_type,erp.creator_id  system_id,lrpr.resource_id,erc.chapter_id,etcs.score  from   neworiental_v3.entity_respackage_publish erp
                          |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                          |join neworiental_v3.entity_res_courseware erc on erc.id=lrpr.resource_id
                          |join neworiental_report.entity_teacher_chapter_score etcs ON etcs.chapter_id=erc.chapter_id and etcs.teacher_system_id=erp.creator_id
                          |where lrpr.resource_type=1 and erc.status=1
                          |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                          |union ALL
                          |--题集
                          |select  DISTINCT 2 resource_type, erp.creator_id  system_id,lrpr.resource_id,erc.chapter_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                          |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                          |join neworiental_v3.entity_exercise erc on erc.id=lrpr.resource_id
                          |join neworiental_report.entity_teacher_chapter_score etcs ON etcs.chapter_id=erc.chapter_id and etcs.teacher_system_id=erp.creator_id
                          |where lrpr.resource_type=2 and erc.status=1
                          |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                          |union ALL
                          |--资料
                          |select  DISTINCT 3 resource_type,erp.creator_id  system_id,lrpr.resource_id,erc.chapter_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                          |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                          |join neworiental_v3.entity_res_clip erc on erc.id=lrpr.resource_id
                          |join neworiental_report.entity_teacher_chapter_score etcs ON etcs.chapter_id=erc.chapter_id and etcs.teacher_system_id=erp.creator_id
                          |where lrpr.resource_type=3 and erc.status=1
                          |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0)
                          |union all
                          |--题目
                          |select  DISTINCT 4 resource_type, erp.creator_id  system_id,leq.question_id resource_id,erc.chapter_id,etcs.score    from   neworiental_v3.entity_respackage_publish erp
                          |join neworiental_v3.link_respackage_publish_resource lrpr on lrpr.publish_id =erp.id
                          |join neworiental_v3.entity_exercise erc on erc.id=lrpr.resource_id
                          |join neworiental_v3.link_exercise_question leq on leq.exercise_id=erc.id
                          |join neworiental_report.entity_teacher_chapter_score etcs ON etcs.chapter_id=erc.chapter_id and etcs.teacher_system_id=erp.creator_id
                          |where lrpr.resource_type=2 and erc.status=1) tmp
                          |group by tmp.chapter_id,tmp.resource_type,tmp.resource_id """.stripMargin
    sql(str_create_cre)

    /*最终表*/
    val str_drop_etre="""  drop table test.entity_teacher_resource_effect """.stripMargin
    sql(str_drop_etre)
    val str_create_etre=s"""  create table test.entity_teacher_resource_effect as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp_l.*,'${DATE}' time from
                          |(select distinct tmp.* from
                          |(
                          |select  distinct  type,type_id,resource_type,resource_id,score  from test.chapter_resource_effect
                          |union all
                          |select distinct type,type_id,resource_type,resource_id,score  from test.topic_resource_effect
                          |) tmp ) tmp_l """.stripMargin
    sql(str_create_etre)
    sc_1.stop()
  }
}
