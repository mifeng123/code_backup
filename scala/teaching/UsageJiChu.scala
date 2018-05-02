package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object UsageJiChu {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("UsageTongJi")
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
    val DATENOW = args(0).toString
    val DAY = args(1).toString
    val DATE = args(2).toString
    val DAYNOW=args(3).toString
    /*基础表开始*/

    /*学生答题*/
    val str_drop_ssa=""" drop table test.school_student_answer """
    sql(str_drop_ssa)
    val str_create_ssa=s""" create table test.school_student_answer as
                          |	select eu.org_id,ese.student_id,ese.question_id,ese.ret_num,ese.subject_ex subject_id,
                          |	ese.exercise_source  exercise_source ,ec.grade_id  grade_id,ec.id class_id,ec.pad_class pad_class,
                          |	ese.date time from student_answer.student_answer_data  ese
                          |	join neworiental_user.entity_user eu  on eu.system_id=ese.student_id
                          |	join
                          |	(
                          |	select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                          |	union all
                          |	select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                          |	) es  on es.org_id=eu.org_id
                          |	join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                          |	join neworiental_user.entity_classes ec on ec.id=lct.class_id
                          |	where  eu.type=2 and lct.checked_status=2 and eu.status=1
                          |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                          |	and ese.ret_num>0 and ese.submit_time is not null and ese.subject_ex>0  and ese.subject_ex is not null
                          |	and ese.submit_time != 'null' and ese.submit_time != 'NULL' and ese.date>='${DATE}' and
                          |	ese.date<'${DATENOW}' and ec.class_type!=3  and ec.class_type!=2 """.stripMargin
    sql(str_create_ssa)
    /*学生浏览 日志*/
    val str_drop_ssb=""" drop table test.school_student_browse """
    sql(str_drop_ssb)
    val str_create_ssb=s""" create table test.school_student_browse as
                          |	select eu.org_id,ese.system_id,etc.subject_id,ec.grade_id,ec.id class_id,ec.pad_class,
                          |	from_unixtime(unix_timestamp(cast(ese.day as string) ,'yyyymmdd'),'yyyy-mm-dd') time from recommend.pad_resouce_read_day ese
                          |	join neworiental_v3.entity_teaching_chapter etc on etc.id=ese.chapter_id
                          |	join neworiental_user.entity_user eu  on eu.system_id=ese.system_id
                          |	join
                          |	(
                          |	select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                          |	union all
                          |	select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                          |	) es  on es.org_id=eu.org_id
                          |	join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                          |	join neworiental_user.entity_classes ec on ec.id=lct.class_id
                          |	where  eu.type=2 and lct.checked_status=2 and eu.status=1 and ec.class_type!=2
                          |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type  and ese.day>='${DAY}' and
                          |	ese.day<'${DAYNOW}' and ec.class_type!=3  """.stripMargin
    sql(str_create_ssb)
    /*学生浏览 数据库表*/
    val str_drop_ssb_sk=""" drop table test.school_student_browse_sk """
    sql(str_drop_ssb_sk)
    val str_create_ssb_sk=s""" create  table test.school_student_browse_sk as
                             |select eu.org_id,erm.student_id system_id,ese.id subject_id,ec.grade_id,ec.id class_id,ec.pad_class,
                             |	cast( to_date(erm.create_time) as string ) time from neworiental_v3.entity_trail_respackage_merged erm
                             |	join neworiental_v3.entity_res_courseware erc on erc.id=erm.resource_id
                             |	join neworiental_v3.entity_subject_ex ese on ese.id=erc.subject_id
                             |	join neworiental_user.entity_user eu  on eu.system_id=erm.student_id
                             |	join
                             |	(
                             |	select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                             |	union all
                             |	select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                             |	) es  on es.org_id=eu.org_id
                             |	join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                             |	join neworiental_user.entity_classes ec on ec.id=lct.class_id
                             |	where  erm.resource_type=1 and eu.type=2 and lct.checked_status=2 and eu.status=1
                             |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type  and to_date(erm.create_time)>='${DATE}' and
                             |	to_date(erm.create_time)<'${DATENOW}'  and ec.class_type!=3 and ec.class_type!=2
                             |union all
                             |select eu.org_id,erm.student_id system_id,ese.id subject_id,ec.grade_id,ec.id class_id,ec.pad_class,
                             |	 cast( to_date(erm.create_time) as string ) time from neworiental_v3.entity_trail_respackage_merged erm
                             |	join neworiental_v3.entity_res_clip erc on erc.id=erm.resource_id
                             |	join neworiental_v3.entity_subject_ex ese on ese.id=erc.subject_id
                             |	join neworiental_user.entity_user eu  on eu.system_id=erm.student_id
                             |	join
                             |	(
                             |	select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                             |	union all
                             |	select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                             |	) es  on es.org_id=eu.org_id
                             |	join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                             |	join neworiental_user.entity_classes ec on ec.id=lct.class_id
                             |	where  erm.resource_type=3 and eu.type=2 and lct.checked_status=2 and eu.status=1
                             |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type  and to_date(erm.create_time)>='${DATE}' and
                             |	to_date(erm.create_time)<'${DATENOW}' and ec.class_type!=3  and ec.class_type!=2  """.stripMargin
    sql(str_create_ssb_sk)
    /*最终浏览数据去上述中数据大的*/
    /*老师上传*/
    val str_drop_tu=""" drop table test.base_teacher_upload  """
    sql(str_drop_tu)
    val str_create_tu=s""" create table test.base_teacher_upload  as
                         |	select distinct erc.id resource_id,erc.created_by system_id,ept.subject_id,erc.org_id,
                         |			ec.grade_id,ec.pad_class,
                         |			case when erc.ext_id is not null or erc.ext_id!='null' or erc.ext_id!='NULL' then erc.ext_id else 0 end ext_id,
                         |			erc.resource_tag resource_tag,to_date(erc.submit_time) time
                         |			from  neworiental_v3.entity_res_clip erc
                         |			join neworiental_user.entity_user eu on eu.system_id=erc.created_by
                         |			join neworiental_user.entity_profile_teacher ept on ept.system_id=eu.system_id
                         |			join
                         |			(select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                         |			union all
                         |			select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                         |			) es on es.org_id=eu.org_id
                         |			join neworiental_user.l_class_teacher lct on lct.teacher_system_id=eu.system_id
                         |			join neworiental_user.entity_classes ec on ec.id=lct.class_id
                         |			where  eu.org_type=2 and eu.type=1 and eu.status=1 and ec.class_type!=2
                         |			and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                         |			and erc.submit_time< '${DATENOW}'   and erc.submit_time>= '${DATE}'
                         |			and erc.resource_tag in(2,3,4,5,6,7,9,11,50) and erc.status=1  and ec.class_type!=3
                         |			union all
                         |			select distinct erc.id resource_id,erc.created_by system_id,ept.subject_id,erc.org_id,
                         |			ec.grade_id,ec.pad_class,0  ext_id,
                         |			1 resource_tag,to_date(erc.submit_time) time
                         |			from  neworiental_v3.entity_res_courseware erc
                         |			join neworiental_user.entity_user eu on eu.system_id=erc.created_by
                         |			join neworiental_user.entity_profile_teacher ept on ept.system_id=eu.system_id
                         |			join
                         |			(select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                         |			union all
                         |			select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                         |			) es on es.org_id=eu.org_id
                         |			join neworiental_user.l_class_teacher lct on lct.teacher_system_id=eu.system_id
                         |			join neworiental_user.entity_classes ec on ec.id=lct.class_id
                         |			where  eu.org_type=2 and eu.type=1 and eu.status=1 and ec.class_type!=2
                         |			and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                         |			and erc.submit_time< '${DATENOW}'   and erc.submit_time>= '${DATE}'
                         |			and erc.status=1 and ec.class_type!=3 """.stripMargin
    sql(str_create_tu)
    /*老师发布*/
    val str_drop_tp=""" drop table test.base_teacher_publish """
    sql(str_drop_tp)
    val str_create_tp=s"""  create table test.base_teacher_publish as
                         | select e.id pu_id,e.publish_type pu_type,eu.org_id org_id,e.creator_id system_id,
                         |      est.id subject_id,est.name subject_name,ec.grade_id grade_id,ec.id  class_id,ec.pad_class pad_class,to_date(e.create_time) time
                         |	from neworiental_v3.entity_respackage_publish  e
                         |	join     neworiental_user.entity_user eu on e.creator_id=eu.system_id
                         |  join     neworiental_user.entity_profile_teacher ept on ept.system_id=eu.system_id
                         |  join     neworiental_v3.entity_subject est on est.id=ept.subject_id
                         |	join
                         |	(
                         |	select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                         |	union all
                         |	select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                         |	) es  on es.org_id=eu.org_id
                         |  join     neworiental_user.l_class_teacher lct on lct.teacher_system_id=eu.system_id
                         |	join     neworiental_user.entity_classes ec on ec.id=lct.class_id
                         |	where   e.publish_type in (1,6,7,8)  and eu.status=1
                         |      and eu.org_id=ec.org_id and eu.org_type=ec.org_type and  ec.class_type!=2 and
                         |			e.create_time<'${DATENOW}'  and e.create_time>= '${DATE}' and e.class_id=ec.id and ec.class_type!=3  """.stripMargin
    sql(str_create_tp)
    /*基础表结束*/
    sc_1.stop()
  }
}
