package coach

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object coachDataCount {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("coachDataCount")
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
    /*val DAYNOW = args(1).toString*/
    val DATE = args(1).toString
    val DATENOW = args(2).toString
    /*教务统计*/
    val str_drop_codd=""" drop table test.coach_office_data_details  """.stripMargin
    sql(str_drop_codd)
    val str_create_codd=s"""  create table test.coach_office_data_details as
                           |select
                           |cast(concat('${DAY}',row_number() over()) as bigint) id,tmp.* from
                           |(select eu.org_id,eu.org_type,ehtp.teacher_id system_id,ept.stage_id,ept.subject_id,
                           |(32-count(DISTINCT ehtp.plan_start_time)) unopen_reservation_num,
                           |count(DISTINCT ehtp.plan_start_time) open_reservation_num,
                           |sum(case when ehr.status in (1,2,4) then 1  else 0 end) response_num,ehtp.plan_date time
                           | from neworiental_v3.entity_help_teacher_plan ehtp
                           |left join neworiental_v3.link_help_teacher_plan_order htpo  on htpo.help_teacher_plan_id=ehtp.id
                           |left join neworiental_v3.entity_help_record ehr on ehr.id=htpo.help_record_id
                           |join neworiental_user.entity_profile_teacher ept on ept.system_id=ehtp.teacher_id
                           |join neworiental_user.entity_user eu on eu.system_id=ept.system_id
                           |where ehtp.status=1 and ehtp.plan_date>='${DATE}' and ehtp.plan_date<'${DATENOW}'
                           |GROUP BY ehtp.plan_date,eu.org_type,eu.org_id,ept.stage_id,ept.subject_id,ehtp.teacher_id) tmp   """.stripMargin
    sql(str_create_codd)
    val str_drop_cods=""" drop table test.coach_office_data_summary """.stripMargin
    sql(str_drop_cods)
    val str_create_cods=s"""
                           |create table test.coach_office_data_summary as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp.* from
                           |(select codd.org_id,codd.org_type,0 stage_id,0 subject_id,
                           |sum(unopen_reservation_num) unopen_reservation_num,sum(open_reservation_num) open_reservation_num,
                           |sum(response_num) response_num ,codd.time
                           |from test.coach_office_data_details codd
                           |where codd.time>='${DATE}' and codd.time<'${DATENOW}'
                           |group by codd.time,codd.org_type,codd.org_id
                           |union all
                           |select codd.org_id,codd.org_type,codd.stage_id,0 subject_id,
                           |sum(unopen_reservation_num) unopen_reservation_num,sum(open_reservation_num) open_reservation_num,
                           |sum(response_num) response_num ,codd.time
                           |from test.coach_office_data_details codd
                           |where codd.time>='${DATE}' and codd.time<'${DATENOW}'
                           |group by codd.time,codd.org_type,codd.org_id,codd.stage_id
                           |union all
                           |select codd.org_id,codd.org_type,codd.stage_id,codd.subject_id,
                           |sum(unopen_reservation_num) unopen_reservation_num,sum(open_reservation_num) open_reservation_num,
                           |sum(response_num) response_num ,codd.time
                           |from test.coach_office_data_details codd
                           |where codd.time>='${DATE}' and codd.time<'${DATENOW}'
                           |group by codd.time,codd.org_type,codd.org_id,codd.stage_id,codd.subject_id) tmp  """.stripMargin
    sql(str_create_cods)
    /*教学统计*/
    /*实际上课数据*/
    val str_drop_acb=""" drop table test.actual_class_base """.stripMargin
    sql(str_drop_acb)
    val str_create_acb=s""" create table test.actual_class_base as
                          |select tmp_ts.publish_id,tmp_ts.teacher_id,
                          | case when tmp_ts.is_big>0 then 1 else 0 end  ts_num,tmp_ts.time from
                          |(select  to_date(etl.create_time) time,etl.publish_id,etl.teacher_id,
                          |(sum(case when etl.create_time=tmp.xk_time then etl.right_rate ELSE 0 end) - sum(case when etl.create_time=tmp.sk_time then etl.right_rate ELSE 0 end)) is_big
                          | from  neworiental_v3.entity_trail_lesson etl
                          |join
                          |(select to_date(create_time)  c_time,publish_id,teacher_id,student_id,min(create_time) sk_time ,max(create_time) xk_time
                          | from neworiental_v3.entity_trail_lesson etl  where create_time>='${DATE}' and create_time<'${DATENOW}'
                          |GROUP BY to_date(create_time),publish_id,teacher_id,student_id ) tmp  on
                          |etl.publish_id=tmp.publish_id and etl.teacher_id=tmp.teacher_id and etl.student_id=tmp.student_id  and tmp.c_time=DATE_FORMAT(etl.create_time,"%Y-%m-%d")
                          |where etl.create_time=tmp.xk_time or etl.create_time=tmp.sk_time
                          |GROUP by  to_date(etl.create_time),etl.publish_id,etl.teacher_id) tmp_ts  """.stripMargin
    sql(str_create_acb)
    /*订单数据*/
    val str_drop_hob=""" drop table test.help_order_base  """.stripMargin
    sql(str_drop_hob)
    val str_create_hob=s""" create table test.help_order_base as
                          |select ehr.status,ehr.publish_id,ehr.teacher_id,eu.org_id,eu.org_type,
                          |ehr.stage_id,ehr.subject_id,ehr.is_second_service,'${DATE}' time
                          | from  neworiental_v3.entity_help_record ehr
                          | join  neworiental_user.entity_user eu on eu.system_id=ehr.teacher_id
                          |where ehr.status in (1,2,3,4) and eu.type=1 and ehr.start_time>='${DATE}' and ehr.start_time<'${DATENOW}' """.stripMargin
    sql(str_create_hob)

    val str_drop_ctdd=""" drop table test.coach_teaching_data_detail  """.stripMargin
    sql(str_drop_ctdd)
    val str_create_ctdd=s"""  create table test.coach_teaching_data_detail as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,hob.org_id,hob.org_type,hob.is_second_service coach_type,hob.teacher_id system_id,hob.stage_id,hob.subject_id,
                           |sum(case when  hob.status in(1,2,3,4) then 1 else 0 end)  help_order_num,
                           |sum(case when acb.publish_id is not null then 1 else 0 end) actual_class_num,
                           |sum(case when acb.publish_id is not null and acb.ts_num=1 then 1 else 0 end) effect_promotion_num,
                           |hob.time
                           |from  test.help_order_base hob
                           |left join test.actual_class_base acb on hob.teacher_id=acb.teacher_id and hob.publish_id=acb.publish_id and hob.time=acb.time and hob.status=2
                           |group by hob.time,hob.org_type,hob.org_id,hob.is_second_service,hob.stage_id,hob.subject_id,hob.teacher_id  """.stripMargin
    sql(str_create_ctdd)

    val str_drop_ctds="""  drop table test.coach_teaching_data_summary """.stripMargin
    sql(str_drop_ctds)
    val str_create_ctds=s"""
                           |create table test.coach_teaching_data_summary as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp.* from
                           |(select  hob.org_id,hob.org_type,
                           |0 stage_id,0 subject_id,hob.is_second_service coach_type,
                           |count(1)  help_order_num,
                           |sum(case when acb.publish_id is not null then 1 else 0 end) actual_class_num,
                           |sum(case when acb.publish_id is not null and acb.ts_num=1 then 1 else 0 end) effect_promotion_num,
                           |hob.time
                           |from  test.help_order_base hob
                           |left join test.actual_class_base acb on hob.teacher_id=acb.teacher_id and hob.publish_id=acb.publish_id and hob.time=acb.time
                           |where hob.time>="${DATE}" and hob.time<"${DATENOW}"
                           |group by hob.time,hob.org_type,hob.org_id,hob.is_second_service
                           |union all
                           |select hob.org_id,hob.org_type,
                           |hob.stage_id,0 subject_id,hob.is_second_service coach_type,
                           |count(1)  help_order_num,
                           |sum(case when acb.publish_id is not null then 1 else 0 end) actual_class_num,
                           |sum(case when acb.publish_id is not null and acb.ts_num=1 then 1 else 0 end) effect_promotion_num,
                           |hob.time
                           |from  test.help_order_base hob
                           |left join test.actual_class_base acb on hob.teacher_id=acb.teacher_id and hob.publish_id=acb.publish_id and hob.time=acb.time
                           |where hob.time>="${DATE}" and hob.time<"${DATENOW}"
                           |group by hob.time,hob.org_type,hob.org_id,hob.is_second_service,hob.stage_id
                           |union all
                           |select hob.org_id,hob.org_type,
                           |hob.stage_id,hob.subject_id,hob.is_second_service coach_type,
                           |count(1)  help_order_num,
                           |sum(case when acb.publish_id is not null then 1 else 0 end) actual_class_num,
                           |sum(case when acb.publish_id is not null and acb.ts_num=1 then 1 else 0 end) effect_promotion_num,
                           |hob.time
                           |from  test.help_order_base hob
                           |left join test.actual_class_base acb on hob.teacher_id=acb.teacher_id and hob.publish_id=acb.publish_id and hob.time=acb.time
                           |where hob.time>="${DATE}" and hob.time<"${DATENOW}"
                           |group by hob.time,hob.org_type,hob.org_id,hob.is_second_service,hob.stage_id,hob.subject_id) tmp  """.stripMargin
    sql(str_create_ctds)
  }
}
