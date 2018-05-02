package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/11/3.
  */
object TeachingWeeklyJiChu {
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
    val DATE6AGO = args(0).toString
    val DATENOW = args(1).toString
    val DAY6AGO = args(2).toString
    val DAY = args(3).toString
    val DATE = args(4).toString
    /* 基础数据*/
     val str_drop_sq=s""" drop table test.student_question """.stripMargin
     sql(str_drop_sq)
     val str_create_sq=s""" create table test.student_question as
                       |select eu.org_id,ese.student_id,ese.question_id,ese.ret_num,est.id subject_id,est.name subject_name,
                       |ese.exercise_source  exercise_source ,ec.grade_id  grade_id,ec.id class_id,ec.pad_class pad_class,
                       |to_date(ese.submit_time) time from neworiental_v3.entity_student_exercise ese
                       |join neworiental_user.entity_user eu  on eu.system_id=ese.student_id
                       |join neworiental_v3.entity_subject est on est.id=ese.subject_ex
                       |join neworiental_logdata.entity_school es  on es.org_id=eu.org_id
                       |join  neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                       |join neworiental_user.entity_classes ec on ec.id=lct.class_id
                       |where es.private=0 and es.enable=1 and eu.type=2 and lct.checked_status=2 and eu.status=1
                       |and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                       |and ese.ret_num>0 and ese.parent_question_id is null	and ese.submit_time is not null
                       |and ese.submit_time != 'null' and ese.submit_time != 'NULL' and ese.submit_time>='${DATE6AGO}' and
                       |ese.submit_time<'${DATENOW}'and ec.class_type!=3 and  ec.class_type!=2 """.stripMargin
     sql(str_create_sq)
     val str_drop_sd=s""" drop table test.student_daily """.stripMargin
     sql(str_drop_sd)
     val str_create_sd=s""" create table test.student_daily as select distinct eu.org_id org_id,eu.system_id system_id,
                          |			ec.grade_id grade_id,ec.id class_id,ec.pad_class pad_class,
                          |			h_system.time time  from
                          |		(select distinct system_id,from_unixtime(unix_timestamp(cast(day as string),'yyyymmdd'),'yyyy-mm-dd') time from recommend.pad_resouce_read_day where day<='${DAY6AGO}' and day>='${DAY}'
                          |		union all
                          |		select  distinct student_id  system_id,to_date(submit_time) time from neworiental_v3.entity_student_exercise
                          |		where ret_num>0 and  parent_question_id is null
                          |		and submit_time is not null and submit_time != 'null' and submit_time != 'NULL'
                          |		and  submit_time>='${DATE6AGO}' and submit_time<'${DATENOW}'
                          |		) h_system
                          |		join neworiental_user.entity_user eu on eu.system_id=h_system.system_id
                          |		join neworiental_logdata.entity_school es on es.org_id=eu.org_id
                          |		join  neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                          |		join neworiental_user.entity_classes ec on ec.id=lct.class_id
                          |		where es.private=0 and es.enable=1 and eu.type=2 and eu.status=1  and eu.org_id=ec.org_id and  eu.org_type=ec.org_type
                          |  and ec.class_type!=3 and   ec.class_type!=2 """.stripMargin
     sql(str_create_sd)
     val str_drop_cd=s""" drop table test.class_daily """.stripMargin
     sql(str_drop_cd)
     val str_create_cd=s""" create  table test.class_daily as
                          |			select distinct eu.org_id ,
                          |			case when ec.grade_id is not null then ec.grade_id else 9999 end grade_id,
                          |			case when ec.id is not null then ec.id else 9999 end class_id,
                          |			case when ec.id is not null then 1 else 0 end class_daily,
                          |			case when ec.pad_class is not null then ec.pad_class else 0 end pad_class,
                          |			from_unixtime(unix_timestamp(cast(apu.day as string),'yyyymmdd'),'yyyy-mm-dd') time
                          |			from  neworiental_report.student_pad_api_user apu
                          |			join neworiental_user.entity_user eu  on eu.system_id=apu.userid
                          |			join neworiental_logdata.entity_school es on es.org_id=eu.org_id
                          |			join  neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                          |			join neworiental_user.entity_classes ec on ec.id=lct.class_id
                          |			where es.private=0 and es.enable=1 and eu.type=2 and lct.checked_status=2 and
                          |			eu.org_id=ec.org_id and eu.org_type=ec.org_type
                          |			and apu.day>='${DAY6AGO}' and  apu.day<='${DAY}' and ec.class_type!=3 and   ec.class_type!=2  """.stripMargin
     sql(str_create_cd)
     val str_drop_sau=s""" drop table test.student_app_user """.stripMargin
     sql(str_drop_sau)
     val str_create_sau=s""" create table test.student_app_user as
                           |select distinct eu.system_id system_id,eu.org_id org_id,
                           |		ec.grade_id grade_id,ec.id class_id,ec.pad_class pad_class,
                           |        case when api like '/api/pad/mcourses/%' then 1
                           |             when api like '/api/pad/workbook/%' then 2  end type,
                           |		from_unixtime(unix_timestamp(cast(day as string),'yyyymmdd'),'yyyy-mm-dd') time
                           |    from neworiental_report.student_pad_api_user a
                           |    join neworiental_user.entity_user eu on a.userid=eu.system_id
                           |	join neworiental_logdata.entity_school es  on es.org_id=eu.org_id
                           |	join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                           |	join neworiental_user.entity_classes ec on ec.id=lct.class_id
                           |    where ( api like '/api/pad/mcourses/%'  or api like '/api/pad/workbook/%')
                           |    and day>='${DAY6AGO}' and day<='${DAY}' and es.private=0 and es.enable=1 and eu.type=2 and eu.status=1
                           |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type and ec.class_type!=3 and   ec.class_type!=2  """.stripMargin
     sql(str_create_sau)
    sc_1.stop()
  }
}
