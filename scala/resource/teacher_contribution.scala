package resource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object teacher_contribution {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("teacher_contribution")
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
    val DAYNOW=args(1).toString
    val DATE = args(2).toString
    val DATENOW=args(3).toString
    /*贡献知识点*/
    val str_drop_ct=""" drop table test.contribution_topic """.stripMargin
    sql(str_drop_ct)
    val str_create_ct=s"""   create table test.contribution_topic as
                         |select tmp.system_id,concat_ws(',',collect_set(cast(tmp.topic_id as STRING))) topic_num,tmp.time from
                         |(
                         |select distinct erc.created_by system_id,et.id topic_id,'${DATE}' time
                         |from neworiental_v3.entity_res_courseware erc
                         |                          join neworiental_v3.link_resource_topic lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_topic et on et.id=lrt.topic_id
                         |where  erc.school_check_status=0 and lrt.resource_type=0 and erc.status=1 and et.status=1 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id topic_id,'${DATE}' time
                         |from neworiental_v3.entity_res_clip erc
                         |                          join neworiental_v3.link_resource_topic lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_topic et on et.id=lrt.topic_id
                         |where  erc.school_check_status=0 and lrt.resource_type=1 and erc.status=1  and et.status=1 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id topic_id,'${DATE}' time
                         |from neworiental_v3.entity_exercise erc
                         |                          join neworiental_v3.link_exercise_topic lrt on lrt.exercise_id=erc.id
                         |                          join neworiental_v3.entity_topic et on et.id=lrt.topic_id
                         |where  erc.school_check_status=0 and erc.category in (30,31,32) and  erc.status=1 and et.status=1 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id topic_id,'${DATE}' time
                         |from neworiental_v3.entity_course_package erc
                         |                          join neworiental_v3.link_resource_topic lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_topic et on et.id=lrt.topic_id
                         |where  erc.school_check_status=0 and lrt.resource_type=30 and erc.status=1 and et.status=1 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.upload_id system_id,et.id topic_id,'${DATE}' time
                         |from neworiental_v3.entity_question erc
                         |                          join neworiental_v3.link_question_topic lrt on lrt.question_id=erc.id
                         |                          join neworiental_v3.entity_topic et on et.id=lrt.topic_id
                         |where  erc.state="ENABLED" and  erc.new_format=1 and erc.parent_question_id=0 and et.status=1  and erc.upload_time>='${DATE}' and erc.upload_time<'${DATENOW}'
                         |) tmp group by tmp.time,tmp.system_id  """.stripMargin
    sql(str_create_ct)
    /*贡献章节*/
    val str_drop_cc="""  drop table test.contribution_chapter  """.stripMargin
    sql(str_drop_cc)
    val str_create_cc=s"""  create table test.contribution_chapter as
                         |select tmp.system_id,concat_ws(',',collect_set(cast(tmp.chapter_id as STRING))) chapter_num,tmp.time from
                         |(
                         |select distinct erc.created_by system_id,et.id chapter_id,'${DATE}' time
                         |from neworiental_v3.entity_res_courseware erc
                         |                          join neworiental_v3.link_resource_chapter lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_teaching_chapter et on et.id=lrt.chapter_id
                         |where  erc.school_check_status=0 and lrt.resource_type=1 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id chapter_id,'${DATE}' time
                         |from neworiental_v3.entity_res_clip erc
                         |                          join neworiental_v3.link_resource_chapter lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_teaching_chapter et on et.id=lrt.chapter_id
                         |where  erc.school_check_status=0 and lrt.resource_type=3 and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id chapter_id,'${DATE}' time
                         |from neworiental_v3.entity_exercise erc
                         |                          join neworiental_v3.link_resource_chapter lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_teaching_chapter et on et.id=lrt.chapter_id
                         |where  erc.school_check_status=0 and lrt.resource_type=2 and erc.category in (30,31,32)  and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.created_by system_id,et.id chapter_id,'${DATE}' time
                         |from neworiental_v3.entity_course_package erc
                         |                          join neworiental_v3.link_resource_chapter lrt on lrt.resource_id=erc.id
                         |                          join neworiental_v3.entity_teaching_chapter et on et.id=lrt.chapter_id
                         |where  erc.school_check_status=0 and lrt.resource_type=4  and erc.submit_time>='${DATE}' and erc.submit_time<'${DATENOW}'
                         |union all
                         |select distinct erc.upload_id system_id,et.id chapter_id,'${DATE}' time
                         |from neworiental_v3.entity_question erc
                         |                          join neworiental_v3.link_question_chapter lrt on lrt.question_id=erc.id
                         |                          join neworiental_v3.entity_teaching_chapter et on et.id=lrt.chapter_id
                         |where  erc.state="ENABLED" and  erc.new_format=1 and erc.parent_question_id=0  and erc.upload_time>='${DATE}' and erc.upload_time<'${DATENOW}'
                         |) tmp group by tmp.time,tmp.system_id """.stripMargin
    sql(str_create_cc)
    /*被引用次数*/
    val str_drop_qt=""" drop table test.quote_data """.stripMargin
    sql(str_drop_qt)
    val str_create_qt=s"""  create table test.quote_data as
                         |select tmp.system_id,sum(tmp.quote_num) quote_num,tmp.time from
                         |(select  erc.created_by system_id ,count(1) quote_num,'${DATE}' time
                         |from neworiental_v3.entity_respackage er
                         |                          join neworiental_user.entity_user eu on eu.system_id=er.creator_id
                         |                          join neworiental_v3.link_respackage_resource lrr on lrr.package_id=er.id
                         |                          join neworiental_v3.entity_res_courseware erc on erc.id=lrr.resource_id
                         |where  erc.school_check_status=0 and lrr.resource_type=1 and erc.status=1 and  erc.created_by!=er.creator_id
                         |and er.create_time>='${DATE}' and er.create_time<'${DATENOW}' and eu.org_id=erc.org_id
                         |GROUP by erc.created_by
                         |union ALL
                         |select  erc.created_by system_id ,count(1) quote_num,'${DATE}' time
                         |from neworiental_v3.entity_respackage er
                         |                          join neworiental_user.entity_user eu on eu.system_id=er.creator_id
                         |                          join neworiental_v3.link_respackage_resource lrr on lrr.package_id=er.id
                         |                          join neworiental_v3.entity_exercise erc on erc.id=lrr.resource_id
                         |where  erc.school_check_status=0 and lrr.resource_type=2 and erc.status=1 and erc.created_by!=er.creator_id
                         |and er.create_time>='${DATE}' and er.create_time<'${DATENOW}' and eu.org_id=erc.org_id
                         |GROUP by erc.created_by
                         |union ALL
                         |select  erc.created_by system_id ,count(1) quote_num,'${DATE}' time
                         |from neworiental_v3.entity_respackage er
                         |						  join neworiental_user.entity_user eu on eu.system_id=er.creator_id
                         |                          join neworiental_v3.link_respackage_resource lrr on lrr.package_id=er.id
                         |                          join neworiental_v3.entity_res_clip erc on erc.id=lrr.resource_id
                         |where  erc.school_check_status=0 and lrr.resource_type=3 and erc.status=1 and erc.created_by!=er.creator_id
                         |and er.create_time>='${DATE}' and er.create_time<'${DATENOW}' and eu.org_id=erc.org_id
                         |GROUP by erc.created_by) tmp GROUP BY tmp.time,tmp.system_id """.stripMargin
    sql(str_create_qt)
    /*匹配学生个数*/
    val str_drop_md=""" drop table  test.match_data """.stripMargin
    sql(str_drop_md)
    val str_create_md=s"""  create table test.match_data as
                         |select tmp.system_id,concat_ws(',',collect_set(cast(tmp.student_id as STRING))) match_student_id,"${DATE}" time from
                         |(select erc.created_by system_id ,eu.system_id student_id,"${DATE}" time from
                         |(select   uid,resource_id
                         |        from logdata.ods_okay_student_pad
                         |        lateral view json_tuple(app_info,"app_id") app as app_id
                         |        lateral view json_tuple(user_info,"uid") user as uid
                         |        lateral view json_tuple(resource_info,"resource_kind","resource_id") resource as resource_kind,resource_id
                         |        where day>="${DAY}" and day<"${DAYNOW}" and  log_type =3 and resource_kind=1  and    resource_info is not null
                         |        and resource_id is not null and resource_id!="null" and resource_id!="NULL" and resource_id!=""
                         |		) tmp
                         |        join neworiental_user.entity_user eu on eu.id=tmp.uid
                         |        join neworiental_v3.entity_res_courseware erc on erc.id=tmp.resource_id
                         |        where erc.school_check_status=0 and erc.status=1 and eu.org_id=erc.org_id
                         |union  all
                         |select  erc.created_by system_id,eu.system_id student_id,"${DATE}" time from
                         |(select  distinct uid,resource_kind,resource_id
                         |        from logdata.ods_okay_student_pad
                         |        lateral view json_tuple(app_info,"app_id") app as app_id
                         |        lateral view json_tuple(user_info,"uid") user as uid
                         |        lateral view json_tuple(resource_info,"resource_kind","resource_id") resource as resource_kind,resource_id
                         |        where day>="${DAY}" and day<"${DAYNOW}" and log_type =3 and   resource_kind not in (1,2) and    resource_info is not null
                         |        and resource_id is not null and resource_id!="null" and resource_id!="NULL" and resource_id!=""
                         |		) tmp
                         |        join neworiental_user.entity_user eu on eu.id=tmp.uid
                         |        join neworiental_v3.entity_res_clip erc on erc.id=tmp.resource_id
                         |        where erc.school_check_status=0 and erc.status=1 and eu.org_id=erc.org_id
                         |union all
                         |select erc.created_by system_id,eu.system_id student_id,"${DATE}" time from neworiental_v3.entity_trail_respackage_merged etrm
                         |join neworiental_user.entity_user eu on eu.system_id=etrm.student_id
                         |join neworiental_v3.entity_res_courseware erc on erc.id=etrm.resource_id
                         |where etrm.resource_type=1 and erc.status=1 and erc.school_check_status=0 and eu.org_id=erc.org_id
                         |union all
                         |select erc.created_by system_id,eu.system_id student_id,"${DATE}" time  from neworiental_v3.entity_trail_respackage_merged etrm
                         |join neworiental_user.entity_user eu on eu.system_id=etrm.student_id
                         |join neworiental_v3.entity_res_clip erc on erc.id=etrm.resource_id
                         |where etrm.resource_type=3 and erc.status=1 and erc.school_check_status=0 and eu.org_id=erc.org_id
                         |union all
                         |select erc.created_by system_id,sad.student_id,"${DATE}" time from student_answer.student_answer_data sad
                         |join neworiental_user.entity_user eu on eu.system_id=sad.student_id
                         |join neworiental_v3.link_respackage_publish_resource lrp on lrp.id=sad.resource_id
                         |join neworiental_v3.entity_exercise erc on erc.id=lrp.resource_id
                         |where sad.exercise_source in (1,6,7,8) and  sad.ret_num>0 and sad.submit_time is not null and sad.subject_ex>0  and sad.subject_ex is not null
                         |and erc.school_check_status=0 and erc.status=1 and sad.submit_time != 'null' and sad.submit_time!= 'NULL' and eu.org_id=erc.org_id   and sad.date>="${DATE}" and sad.date<"${DATENOW}"
                         |union all
                         |select erc.upload_id system_id,sad.student_id,"${DATE}" time from student_answer.student_answer_data sad
                         |join neworiental_user.entity_user eu on eu.system_id=sad.student_id
                         |join neworiental_v3.entity_question erc on erc.id=sad.question_id
                         |where sad.exercise_source not  in (1,6,7,8) and  sad.ret_num>0 and sad.submit_time is not null and sad.subject_ex>0  and sad.subject_ex is not null
                         |and sad.submit_time != 'null' and sad.submit_time!= 'NULL' and   erc.state="ENABLED" and  erc.new_format=1 and erc.parent_question_id=0
                         |and eu.org_id=erc.org_id  and sad.date>="${DATE}" and sad.date<"${DATENOW}" ) tmp
                         |group by tmp.time,tmp.system_id """.stripMargin
    sql(str_create_md)
    /*贡献知识点汇总表*/
    val str_drop_ttcu=""" drop table test.teacher_topic_contribution_usage """.stripMargin
    sql(str_drop_ttcu)
    val str_create_ttcu=s"""  create table test.teacher_topic_contribution_usage as  
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp.* from 
                           | (select distinct  tb.system_id,tb.org_id,tb.org_type,tb.stage_id,tb.grade_id,tb.grade_subject_name,tb.subject_id,
                           | case when ct.topic_num is not null then ct.topic_num else 0 end  topic_id, 
                           | case when cc.chapter_num is not null then cc.chapter_num else 0 end  chapter_id, 
                           | case when qd.quote_num is not null then qd.quote_num else 0 end  quote_num,
                           | case when ma.match_student_id is not null then ma.match_student_id else 0 end  match_student_id, 
                           | tb.time
                           | from test.teacher_basic tb
                           | left join test.contribution_chapter cc on cc.system_id=tb.system_id
                           | left join test.contribution_topic ct on ct.system_id=tb.system_id
                           | left join test.quote_data qd on qd.system_id=tb.system_id
                           | left join test.match_data ma on ma.system_id=tb.system_id) tmp """.stripMargin
    sql(str_create_ttcu)
    /*上传资源基础表*/
    val str_drop_rb=""" drop table test.resource_basic  """.stripMargin
    sql(str_drop_rb)
    val str_create_rb=s""" create table test.resource_basic as
                         |select tmp.system_id,tmp.reource_type,count(DISTINCT tmp.id) resource_num,tmp.time from
                         |(select erc.created_by system_id,1 reource_type,
                         |erc.id,to_date(erc.submit_time) time from neworiental_v3.entity_res_courseware erc
                         |where erc.school_check_status=0 and erc.status=1 and erc.submit_time>="${DATE}" and erc.submit_time<"${DATENOW}"
                         |union all
                         |select erc.created_by system_id,
                         |case when  erc.ext_id>0 and erc.resource_tag in (5,9) then 6
                         |else  4 end reource_type,
                         |erc.id,to_date(erc.submit_time) time from neworiental_v3.entity_res_clip erc
                         |where erc.school_check_status=0 and erc.status=1 and erc.submit_time>="${DATE}" and erc.submit_time<"${DATENOW}"
                         |union all
                         |select  erc.created_by system_id,2 reource_type,erc.id,to_date(erc.submit_time) time from   neworiental_v3.entity_exercise erc
                         |where erc.school_check_status=0 and erc.status=1 and erc.submit_time>="${DATE}" and erc.submit_time<"${DATENOW}"
                         |union all
                         |select erc.created_by system_id,3 reource_type,erc.id,to_date(erc.submit_time) time  from neworiental_v3.entity_course_package erc
                         |where erc.school_check_status=0 and erc.status=1 and erc.submit_time>="${DATE}" and erc.submit_time<"${DATENOW}"
                         |union all
                         |select erc.upload_id system_id,5 reource_type,erc.id,to_date(erc.upload_time) time from neworiental_v3.entity_question erc
                         |where erc.state="ENABLED" and  erc.new_format=1 and erc.parent_question_id=0 and erc.upload_time>="${DATE}" and erc.upload_time<"${DATENOW}" ) tmp
                         |GROUP BY tmp.time,tmp.system_id,tmp.reource_type  """.stripMargin
    sql(str_create_rb)
    /*上传资源汇总表*/
    val str_drop_trcu=""" drop table test.teacher_resource_contribution_usage  """.stripMargin
    sql(str_drop_trcu)
    val str_create_trcu=s""" create table test.teacher_resource_contribution_usage as
                           | select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp.* from
                           | (select distinct tb.system_id,tb.org_id,tb.org_type,tb.stage_id,tb.grade_id,tb.grade_subject_name,tb.subject_id,
                           | case when rb.reource_type is not null then rb.reource_type else 0 end  reource_tag,
                           | case when rb.reource_type is not null then rb.resource_num else 0 end  resource_num,
                           | tb.time
                           | from test.teacher_basic tb
                           | left join test.resource_basic rb on rb.system_id=tb.system_id) tmp """.stripMargin
    sql(str_create_trcu)
    sc_1.stop()
  }
}
