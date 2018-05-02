package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Usage {
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
    /*老师每天发布*/
    val str_drop_tpd=""" drop table test.teacher_publish_day """
    sql(str_drop_tpd)
    val str_create_tpd=s""" create table test.teacher_publish_day as
                         |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                         |(select distinct  h.org_id,h.grade_id,h.pad_class is_pad,2 public_private,h.fb_number_all,
                         |        h.fb_sk_number,h.fb_zy_number,h.fb_cp_number,h.fb_yx_number,
                         |		h.fb_user_all,h.fb_sk_user,h.fb_zy_user,h.fb_cp_user,h.fb_yx_user,h.time
                         | from
                         |	(select org_id,0 grade_id,9999 pad_class,
                         |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                         |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                         |
                         |				count(DISTINCT system_id) fb_user_all,
                         |				count(DISTINCT CASE WHEN pu_type=1 then system_id END) fb_sk_user,
                         |				count(DISTINCT CASE WHEN pu_type=6 then system_id END) fb_zy_user,
                         |				count(DISTINCT CASE WHEN pu_type=7 then system_id END) fb_cp_user,
                         |				count(DISTINCT CASE WHEN pu_type=8 then system_id END) fb_yx_user,
                         |				time
                         |				from test.base_teacher_publish
                         |				group by time,org_id
                         |	union all
                         |	select org_id,0 grade_id,pad_class,
                         |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                         |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                         |
                         |				count(DISTINCT system_id) fb_user_all,
                         |				count(DISTINCT CASE WHEN pu_type=1 then system_id END) fb_sk_user,
                         |				count(DISTINCT CASE WHEN pu_type=6 then system_id END) fb_zy_user,
                         |				count(DISTINCT CASE WHEN pu_type=7 then system_id END) fb_cp_user,
                         |				count(DISTINCT CASE WHEN pu_type=8 then system_id END) fb_yx_user,
                         |				time
                         |				from test.base_teacher_publish
                         |				group by time,org_id,pad_class
                         |    union all
                         |    select org_id,grade_id,9999 pad_class,
                         |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                         |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                         |
                         |				count(DISTINCT system_id) fb_user_all,
                         |				count(DISTINCT CASE WHEN pu_type=1 then system_id END) fb_sk_user,
                         |				count(DISTINCT CASE WHEN pu_type=6 then system_id END) fb_zy_user,
                         |				count(DISTINCT CASE WHEN pu_type=7 then system_id END) fb_cp_user,
                         |				count(DISTINCT CASE WHEN pu_type=8 then system_id END) fb_yx_user,
                         |				time
                         |				from test.base_teacher_publish
                         |				group by time,org_id,grade_id
                         |	union all
                         |    select org_id,grade_id,pad_class,
                         |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                         |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                         |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                         |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                         |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                         |
                         |				count(DISTINCT system_id) fb_user_all,
                         |				count(DISTINCT CASE WHEN pu_type=1 then system_id END) fb_sk_user,
                         |				count(DISTINCT CASE WHEN pu_type=6 then system_id END) fb_zy_user,
                         |				count(DISTINCT CASE WHEN pu_type=7 then system_id END) fb_cp_user,
                         |				count(DISTINCT CASE WHEN pu_type=8 then system_id END) fb_yx_user,
                         |				time
                         |				from test.base_teacher_publish
                         |				group by time,org_id,grade_id,pad_class	)h ) h1 """.stripMargin
    sql(str_create_tpd)
    /*学生浏览数数据每天*/
    val str_drop_sbd=""" drop table test.student_browse_day """
    sql(str_drop_sbd)
    val str_create_sbd="""  create table test.student_browse_day as
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |					 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |			 0 grade_id,9999 pad_class,
                         |			 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id,0 grade_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id) bro_rz
                         |			full outer join
                         |			(select org_id,0 grade_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id
                         |union all
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |				case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |				 0 grade_id,
                         |				 case when bro_rz.pad_class is not null then bro_rz.pad_class else bro_sk.pad_class end pad_class,
                         |				 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id,0 grade_id, pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id,pad_class) bro_rz
                         |			full outer join
                         |			(select org_id,0 grade_id, pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id,pad_class) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id and bro_rz.pad_class=bro_sk.pad_class
                         |union all
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |					 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |			  case when bro_rz.grade_id is not null then bro_rz.grade_id else bro_sk.grade_id end grade_id,
                         |			 9999 pad_class ,
                         |			 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id, grade_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id,grade_id) bro_rz
                         |			full outer join
                         |			(select org_id, grade_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id,grade_id) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id and bro_rz.grade_id=bro_sk.grade_id
                         |union all
                         |select
                         |					 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |						  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |						  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |							   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |						  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |							   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |						 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |				  case when bro_rz.grade_id is not null then bro_rz.grade_id else bro_sk.grade_id end grade_id,
                         |				  case when bro_rz.pad_class is not null then bro_rz.pad_class else bro_sk.pad_class end pad_class,
                         |				  case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |				from
                         |				(select org_id, grade_id,pad_class,
                         |								count(system_id) browse_all_num,
                         |								time from test.school_student_browse
                         |								group by time,org_id,grade_id,pad_class) bro_rz
                         |				full outer join
                         |				(select org_id, grade_id,pad_class,
                         |								count(system_id) browse_all_num,
                         |								time from test.school_student_browse_sk
                         |								group by time,org_id,grade_id,pad_class) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id
                         |								and bro_rz.grade_id=bro_sk.grade_id and bro_rz.pad_class=bro_sk.pad_class """.stripMargin
    sql(str_create_sbd)
   /*学生每天答题和浏览 汇合*/
    val str_drop_sabd=""" drop table test.student_answer_browse_day  """
    sql(str_drop_sabd)
    val str_create_sabd=s"""  create table test.student_answer_browse_day as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                           | (select distinct  h.* from
                           |	 (select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			0 grade_id, 9999 is_pad,2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,0 grade_id,9999 pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id
                           |			) ans
                           |			full outer join
                           |			(
                           |			 select  browse_all_num,org_id, grade_id, pad_class, time
                           |			  from test.student_browse_day
                           |			  where pad_class=9999 and grade_id=0
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			0 grade_id,
                           |			 case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end is_pad,
                           |			2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,0 grade_id,pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,pad_class
                           |			) ans
                           |			full outer join
                           |			(
                           |			   select  browse_all_num,org_id, grade_id, pad_class, time
                           |			  from test.student_browse_day
                           |			  where pad_class!=9999 and grade_id=0
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.pad_class=bro.pad_class
                           | 	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end  grade_id,
                           |			9999 is_pad,2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,grade_id,9999 pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,grade_id
                           |			) ans
                           |			full outer join
                           |			(   select  browse_all_num,org_id, grade_id, pad_class, time
                           |			  from test.student_browse_day
                           |			  where pad_class=9999 and grade_id!=0
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and  ans.grade_id=bro.grade_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end  grade_id,
                           |			case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end  is_pad,
                           |			2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,grade_id, pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,grade_id,pad_class
                           |			) ans
                           |			full outer join
                           |			(
                           |			select  browse_all_num,org_id, grade_id, pad_class, time
                           |			  from test.student_browse_day
                           |			  where pad_class!=9999 and grade_id!=0
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and  ans.grade_id=bro.grade_id and ans.pad_class=bro.pad_class
                           |			) h) h1  """.stripMargin
    sql(str_create_sabd)
    /*老师发布学生答题每科*/
    val str_drop_saps=""" drop table test.student_answer_publish_subject """
    sql(str_drop_saps)
    val str_create_saps=s""" create table test.student_answer_publish_subject as
                           | select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                           |    (select distinct h.* from
                           |	 (select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |				0 grade_id,
                           |				case when bro.subject_id is not null then  bro.subject_id else ans.subject_id  end subject_id,
                           |				9999 is_pad,2 public_private,
                           |				case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |				case when bro.fb_num is not null then  bro.fb_num else 0 end fb_num,
                           |				case when bro.time is not null then  bro.time else ans.time  end time
                           |		 from
                           |				(select org_id,0 grade_id,9999 pad_class,subject_id,
                           |					count(question_id) answer_all_num,
                           |					time from test.school_student_answer
                           |					group by time,org_id,subject_id
                           |				) ans
                           |				full outer join
                           |				(select org_id,0 grade_id,9999 pad_class,subject_id,
                           |					count(pu_id) fb_num,
                           |					time
                           |					from test.base_teacher_publish
                           |					group by time,org_id,subject_id
                           |				) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and 	ans.subject_id=bro.subject_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |				0 grade_id,
                           |				case when bro.subject_id is not null then  bro.subject_id else ans.subject_id  end subject_id,
                           |				case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end  is_pad,
                           |				2 public_private,
                           |				case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |				case when bro.fb_num is not null then  bro.fb_num else 0 end fb_num,
                           |				case when bro.time is not null then  bro.time else ans.time  end time
                           |		 from
                           |				(select org_id,0 grade_id,pad_class,subject_id,
                           |					count(question_id) answer_all_num,
                           |					time from test.school_student_answer
                           |					group by time,org_id,subject_id,pad_class
                           |				) ans
                           |				full outer join
                           |				(select org_id,0 grade_id,pad_class,subject_id,
                           |					count(pu_id) fb_num,
                           |					time
                           |					from test.base_teacher_publish
                           |					group by time,org_id,subject_id,pad_class
                           |				) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and 	ans.subject_id=bro.subject_id and ans.pad_class=bro.pad_class
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |				case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end   grade_id,
                           |				case when bro.subject_id is not null then  bro.subject_id else ans.subject_id  end subject_id,
                           |				9999 is_pad,2 public_private,
                           |				case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |				case when bro.fb_num is not null then  bro.fb_num else 0 end fb_num,
                           |				case when bro.time is not null then  bro.time else ans.time  end time
                           |		 from
                           |				(select org_id,grade_id,9999 pad_class,subject_id,
                           |					count(question_id) answer_all_num,
                           |					time from test.school_student_answer
                           |					group by time,org_id,subject_id,grade_id
                           |				) ans
                           |				full outer join
                           |				(select org_id,grade_id,9999 pad_class,subject_id,
                           |					count(pu_id) fb_num,
                           |					time
                           |					from test.base_teacher_publish
                           |					group by time,org_id,subject_id,grade_id
                           |				) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and 	ans.subject_id=bro.subject_id and ans.grade_id=bro.grade_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |				case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end   grade_id,
                           |				case when bro.subject_id is not null then  bro.subject_id else ans.subject_id  end subject_id,
                           |				case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end  is_pad,
                           |				2 public_private,
                           |				case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |				case when bro.fb_num is not null then  bro.fb_num else 0 end fb_num,
                           |				case when bro.time is not null then  bro.time else ans.time  end time
                           |		 from
                           |				(select org_id,grade_id,  pad_class,subject_id,
                           |					count(question_id) answer_all_num,
                           |					time from test.school_student_answer
                           |					group by time,org_id,subject_id,grade_id,pad_class
                           |				) ans
                           |				full outer join
                           |				(select org_id,grade_id,pad_class,subject_id,
                           |					count(pu_id) fb_num,
                           |					time
                           |					from test.base_teacher_publish
                           |					group by time,org_id,subject_id,grade_id,pad_class
                           |				) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and 	ans.subject_id=bro.subject_id and ans.grade_id=bro.grade_id and ans.pad_class=bro.pad_class
                           |				) h) h1 """.stripMargin
    sql(str_create_saps)
    /*学生浏览数据 班级*/
    val str_drop_sbc=""" drop table test.student_browse_class  """
    sql(str_drop_sbc)
    val str_create_sbc=""" create table test.student_browse_class as
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |					 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |			 0 grade_id,
                         |			 case when bro_rz.class_id is not null then bro_rz.class_id else bro_sk.class_id end class_id,
                         |			 9999 pad_class ,
                         |			 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id,0 grade_id,class_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id,class_id) bro_rz
                         |			full outer join
                         |			(select org_id,0 grade_id,class_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id,class_id) bro_sk on bro_rz.time=bro_sk.time and
                         |							bro_rz.org_id=bro_sk.org_id and bro_rz.class_id=bro_sk.class_id
                         |union all
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |				case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |				 0 grade_id,
                         |				 case when bro_rz.class_id is not null then bro_rz.class_id else bro_sk.class_id end class_id,
                         |				 case when bro_rz.pad_class is not null then bro_rz.pad_class else bro_sk.pad_class end pad_class,
                         |				 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id,0 grade_id,class_id, pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id,pad_class,class_id) bro_rz
                         |			full outer join
                         |			(select org_id,0 grade_id,class_id, pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id,pad_class,class_id) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id
                         |							and bro_rz.pad_class=bro_sk.pad_class and bro_rz.class_id=bro_sk.class_id
                         |union all
                         |select
                         |				 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |					  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |						   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |					 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |			  case when bro_rz.grade_id is not null then bro_rz.grade_id else bro_sk.grade_id end grade_id,
                         |			  case when bro_rz.class_id is not null then bro_rz.class_id else bro_sk.class_id end class_id,
                         |			 9999 pad_class ,
                         |			 case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |			from
                         |			(select org_id, grade_id,class_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse
                         |							group by time,org_id,grade_id,class_id) bro_rz
                         |			full outer join
                         |			(select org_id, grade_id,class_id,9999 pad_class,
                         |							count(system_id) browse_all_num,
                         |							time from test.school_student_browse_sk
                         |							group by time,org_id,grade_id,class_id) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id
                         |							and bro_rz.grade_id=bro_sk.grade_id and bro_rz.class_id=bro_sk.class_id
                         |union all
                         |select
                         |					 case when bro_rz.browse_all_num is not null and bro_sk.browse_all_num is null then bro_rz.browse_all_num
                         |						  when bro_rz.browse_all_num is null and bro_sk.browse_all_num is not null then bro_sk.browse_all_num
                         |						  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |							   and bro_rz.browse_all_num>=bro_sk.browse_all_num then bro_rz.browse_all_num
                         |						  when bro_rz.browse_all_num is not  null and bro_sk.browse_all_num is not null
                         |							   and bro_rz.browse_all_num<bro_sk.browse_all_num then bro_sk.browse_all_num end  browse_all_num ,
                         |						 case when bro_rz.org_id is not null then bro_rz.org_id else bro_sk.org_id end org_id,
                         |				  case when bro_rz.grade_id is not null then bro_rz.grade_id else bro_sk.grade_id end grade_id,
                         |				  case when bro_rz.class_id is not null then bro_rz.class_id else bro_sk.class_id end class_id,
                         |				  case when bro_rz.pad_class is not null then bro_rz.pad_class else bro_sk.pad_class end pad_class,
                         |				   case when bro_rz.time is not null then bro_rz.time else bro_sk.time end time
                         |				from
                         |				(select org_id, grade_id,class_id,pad_class,
                         |								count(system_id) browse_all_num,
                         |								time from test.school_student_browse
                         |								group by time,org_id,grade_id,pad_class,class_id) bro_rz
                         |				full outer join
                         |				(select org_id, grade_id,class_id,pad_class,
                         |								count(system_id) browse_all_num,
                         |								time from test.school_student_browse_sk
                         |								group by time,org_id,grade_id,pad_class,class_id) bro_sk on bro_rz.time=bro_sk.time and bro_rz.org_id=bro_sk.org_id
                         |								and bro_rz.grade_id=bro_sk.grade_id and bro_rz.pad_class=bro_sk.pad_class and bro_rz.class_id=bro_sk.class_id  """.stripMargin
    sql(str_create_sbc)
    /*学生班级每天答题和浏览*/
    val str_drop_sabc=""" drop table test.student_answer_browse_class """
    sql(str_drop_sabc)
    val str_create_sabc=s""" create table test.student_answer_browse_class as
                           |  select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                           | (select distinct  h.* from
                           |	 (select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			0 grade_id,
                           |			case when bro.class_id is not null then  bro.class_id else ans.class_id end class_id,
                           |			9999 is_pad,2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,0 grade_id,class_id,9999 pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,class_id
                           |			) ans
                           |			full outer join
                           |			(
                           |			select browse_all_num , org_id, grade_id, class_id,pad_class ,time
                           |			from test.student_browse_class where grade_id=0 and pad_class=9999
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.class_id=bro.class_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			0 grade_id,
                           |			case when bro.class_id is not null then  bro.class_id else ans.class_id end class_id,
                           |			 case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end is_pad,
                           |			2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,0 grade_id,class_id,pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,pad_class,class_id
                           |			) ans
                           |			full outer join
                           |			(
                           |			 select browse_all_num , org_id, grade_id, class_id,pad_class ,time
                           |			from test.student_browse_class where grade_id=0 and pad_class!=9999
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.pad_class=bro.pad_class and ans.class_id=bro.class_id
                           | 	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end  grade_id,
                           |			case when bro.class_id is not null then  bro.class_id else ans.class_id end class_id,
                           |			9999 is_pad,2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,grade_id,class_id,9999 pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,grade_id,class_id
                           |			) ans
                           |			full outer join
                           |			(
                           |			 select browse_all_num , org_id, grade_id, class_id,pad_class ,time
                           |			from test.student_browse_class where grade_id!=0 and pad_class=9999
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and  ans.grade_id=bro.grade_id and ans.class_id=bro.class_id
                           |	union all
                           |	select distinct  case when ans.org_id is not null then  ans.org_id else bro.org_id end org_id,
                           |			case when bro.grade_id is not null then  bro.grade_id else ans.grade_id  end  grade_id,
                           |			case when bro.class_id is not null then  bro.class_id else ans.class_id end class_id,
                           |			case when bro.pad_class is not null then  bro.pad_class else ans.pad_class  end  is_pad,
                           |			2 public_private,
                           |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                           |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                           |			case when bro.time is not null then  bro.time else ans.time  end time
                           |	 from
                           |			(select org_id,grade_id, class_id,pad_class,
                           |				count(question_id) answer_all_num,
                           |				time from test.school_student_answer
                           |				group by time,org_id,grade_id,pad_class,class_id
                           |			) ans
                           |			full outer join
                           |			(
                           |			select browse_all_num , org_id, grade_id, class_id,pad_class ,time
                           |			from test.student_browse_class where grade_id!=0 and pad_class!=9999
                           |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and  ans.grade_id=bro.grade_id and ans.pad_class=bro.pad_class and ans.class_id=bro.class_id
                           |			) h) h1 """.stripMargin
    sql(str_create_sabc)
    /*学科发布上传*/
    val str_drop_tpus=""" drop table test.teacher_publish_upload_subject """
    sql(str_drop_tpus)
    val str_create_tpus=s""" create table test.teacher_publish_upload_subject as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                           |	(select distinct h.* from
                           |	(select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                           |			0 grade_id,
                           |			9999 is_pad,2 public_private,
                           |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                           |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                           |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                           |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                           |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                           |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                           |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                           |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                           |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                           |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                           |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                           |			case when pub.time is not null then  pub.time else upl.time  end time
                           |	from
                           |	(select org_id,0 grade_id,9999 pad_class,subject_id,
                           |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                           |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                           |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                           |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                           |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                           |				time
                           |				from test.base_teacher_publish
                           |			 	group by time,org_id,subject_id) pub
                           |	---上传学校 学科
                           |	full outer join
                           |	(select org_id,0 grade_id,9999 pad_class,subject_id,
                           |	            count(1) sc_number_all,
                           |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                           |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                           |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                           |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                           |				time
                           |				from test.base_teacher_upload
                           |				group BY time,org_id,subject_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id
                           |	union all
                           |	--学校 pad班
                           |    select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                           |			0 grade_id,
                           |			case when pub.pad_class is not null then  pub.pad_class else upl.pad_class end  is_pad,
                           |			2 public_private,
                           |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                           |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                           |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                           |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                           |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                           |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                           |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                           |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                           |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                           |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                           |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                           |			case when pub.time is not null then  pub.time else upl.time  end time
                           |	from
                           |	(select org_id,0 grade_id,pad_class,subject_id,
                           |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                           |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                           |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                           |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                           |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                           |				time
                           |				from test.base_teacher_publish
                           |			 	group by time,org_id,subject_id,pad_class) pub
                           |	---上传学校 学科
                           |	full outer join
                           |	(select org_id,0 grade_id,pad_class,subject_id,
                           |	            count(1) sc_number_all,
                           |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                           |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                           |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                           |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                           |				time
                           |				from test.base_teacher_upload
                           |				group BY time,org_id,subject_id,pad_class) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id	and pub.pad_class=upl.pad_class
                           |	union all
                           |	--年级
                           |	select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                           |			case when pub.grade_id is not null then  pub.grade_id else upl.grade_id end grade_id,
                           |			9999 is_pad,2 public_private,
                           |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                           |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                           |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                           |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                           |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                           |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                           |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                           |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                           |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                           |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                           |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                           |			case when pub.time is not null then  pub.time else upl.time  end time
                           |	from
                           |	(select org_id, grade_id,9999 pad_class,subject_id,
                           |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                           |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                           |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                           |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                           |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                           |				time
                           |				from test.base_teacher_publish
                           |			 	group by time,org_id,subject_id,grade_id) pub
                           |	---上传学校 学科
                           |	full outer join
                           |	(select org_id, grade_id,9999 pad_class,subject_id,
                           |	            count(1) sc_number_all,
                           |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                           |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                           |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                           |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                           |				time
                           |				from test.base_teacher_upload
                           |				group BY time,org_id,subject_id,grade_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id and pub.grade_id=upl.grade_id
                           |	union all
                           |	---年级 pad班
                           |	select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                           |			case when pub.grade_id is not null then  pub.grade_id else upl.grade_id end grade_id,
                           |			case when pub.pad_class is not null then  pub.pad_class else upl.pad_class end is_pad,
                           |			2 public_private,
                           |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                           |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                           |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                           |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                           |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                           |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                           |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                           |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                           |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                           |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                           |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                           |			case when pub.time is not null then  pub.time else upl.time  end time
                           |	from
                           |	(select org_id, grade_id, pad_class,subject_id,
                           |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                           |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                           |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                           |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                           |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                           |				time
                           |				from test.base_teacher_publish
                           |			 	group by time,org_id,subject_id,grade_id,pad_class) pub
                           |	---上传学校 学科
                           |	full outer join
                           |	(select org_id, grade_id, pad_class,subject_id,
                           |	            count(1) sc_number_all,
                           |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                           |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                           |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                           |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                           |				time
                           |				from test.base_teacher_upload
                           |				group BY time,org_id,subject_id,grade_id,pad_class) upl on pub.time=upl.time and pub.org_id=upl.org_id and
                           |				pub.subject_id=upl.subject_id and pub.grade_id=upl.grade_id and pub.pad_class=upl.pad_class
                           |				) h) h1 """.stripMargin
    sql(str_create_tpus)
    /*学科下老师发布*/
    val str_drop_tpust=""" drop table test.teacher_publish_upload_subject_teacher """
    sql(str_drop_tpust)
    val str_create_tpust=s""" create table test.teacher_publish_upload_subject_teacher as
                            | select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                            | (select distinct h.* from
                            |	(select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                            |			0 grade_id,
                            |			case when pub.system_id is not null then  pub.system_id else upl.system_id end system_id,
                            |			9999 is_pad,2 public_private,
                            |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                            |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                            |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                            |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                            |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                            |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                            |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                            |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                            |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                            |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                            |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                            |			case when pub.time is not null then  pub.time else upl.time  end time
                            |	from
                            |	(select org_id,0 grade_id,system_id,9999 pad_class,subject_id,
                            |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                            |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                            |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                            |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                            |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                            |				time
                            |				from test.base_teacher_publish
                            |			 	group by time,org_id,subject_id,system_id) pub
                            |	---上传学校 学科
                            |	full outer join
                            |	(select org_id,0 grade_id,system_id,9999 pad_class,subject_id,
                            |	            count(1) sc_number_all,
                            |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                            |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                            |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                            |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                            |				time
                            |				from test.base_teacher_upload
                            |				group BY time,org_id,subject_id,system_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id	and pub.system_id=upl.system_id
                            |	union all
                            |	--学校 pad班
                            |    select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                            |			0 grade_id,
                            |			case when pub.system_id is not null then  pub.system_id else upl.system_id end system_id,
                            |			case when pub.pad_class is not null then  pub.pad_class else upl.pad_class end  is_pad,
                            |			2 public_private,
                            |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                            |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                            |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                            |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                            |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                            |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                            |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                            |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                            |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                            |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                            |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                            |			case when pub.time is not null then  pub.time else upl.time  end time
                            |	from
                            |	(select org_id,0 grade_id,system_id,pad_class,subject_id,
                            |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                            |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                            |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                            |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                            |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                            |				time
                            |				from test.base_teacher_publish
                            |			 	group by time,org_id,subject_id,pad_class,system_id) pub
                            |	---上传学校 学科
                            |	full outer join
                            |	(select org_id,0 grade_id,system_id,pad_class,subject_id,
                            |	            count(1) sc_number_all,
                            |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                            |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                            |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                            |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                            |				time
                            |				from test.base_teacher_upload
                            |				group BY time,org_id,subject_id,pad_class,system_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id
                            |				and pub.pad_class=upl.pad_class	and pub.system_id=upl.system_id
                            |	union all
                            |	--年级
                            |	select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                            |			case when pub.grade_id is not null then  pub.grade_id else upl.grade_id end grade_id,
                            |			case when pub.system_id is not null then  pub.system_id else upl.system_id end system_id,
                            |			9999 is_pad,2 public_private,
                            |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                            |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                            |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                            |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                            |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                            |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                            |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                            |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                            |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                            |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                            |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                            |			case when pub.time is not null then  pub.time else upl.time  end time
                            |	from
                            |	(select org_id, grade_id,system_id,9999 pad_class,subject_id,
                            |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                            |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                            |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                            |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                            |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                            |				time
                            |				from test.base_teacher_publish
                            |			 	group by time,org_id,subject_id,grade_id,system_id) pub
                            |	---上传学校 学科
                            |	full outer join
                            |	(select org_id, grade_id,system_id,9999 pad_class,subject_id,
                            |	            count(1) sc_number_all,
                            |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                            |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                            |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                            |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                            |				time
                            |				from test.base_teacher_upload
                            |				group BY time,org_id,subject_id,grade_id,system_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and pub.subject_id=upl.subject_id
                            |				and pub.grade_id=upl.grade_id and pub.system_id=upl.system_id
                            |	union all
                            |	---年级 pad班
                            |	select distinct  case when pub.org_id is not null then  pub.org_id else upl.org_id end org_id,
                            |			case when pub.grade_id is not null then  pub.grade_id else upl.grade_id end grade_id,
                            |			case when pub.system_id is not null then  pub.system_id else upl.system_id end system_id,
                            |			case when pub.pad_class is not null then  pub.pad_class else upl.pad_class end is_pad,
                            |			2 public_private,
                            |			case when pub.subject_id is not null then  pub.subject_id else upl.subject_id end subject_id,
                            |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number_all,
                            |			case when pub.fb_sk_number is not null then  pub.fb_sk_number else 0 end fb_sk_number,
                            |			case when pub.fb_zy_number is not null then  pub.fb_zy_number else 0 end fb_zy_number,
                            |			case when pub.fb_cp_number is not null then  pub.fb_cp_number else 0 end fb_cp_number,
                            |			case when pub.fb_yx_number is not null then  pub.fb_yx_number else 0 end fb_yx_number,
                            |			case when upl.sc_number_all is not null then  upl.sc_number_all else 0 end sc_number_all,
                            |			case when upl.sc_dxa_number is not null then  upl.sc_dxa_number else 0 end sc_dxa_number,
                            |			case when upl.sc_ja_number is not null then  upl.sc_ja_number else 0 end sc_ja_number,
                            |			case when upl.sc_kj_number is not null then  upl.sc_kj_number else 0 end sc_kj_number,
                            |			case when upl.sc_other_number is not null then  upl.sc_other_number else 0 end sc_other_number,
                            |			case when pub.time is not null then  pub.time else upl.time  end time
                            |	from
                            |	(select org_id, grade_id,system_id, pad_class,subject_id,
                            |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                            |				sum(CASE WHEN pu_type=1 then 1   else 0 END) fb_sk_number,
                            |				sum(CASE WHEN pu_type=6 then 1   else 0 END) fb_zy_number,
                            |				sum(CASE WHEN pu_type=7 then 1   else 0 END) fb_cp_number,
                            |				sum(CASE WHEN pu_type=8 then 1   else 0 END) fb_yx_number,
                            |				time
                            |				from test.base_teacher_publish
                            |			 	group by time,org_id,subject_id,grade_id,pad_class,system_id) pub
                            |	---上传学校 学科
                            |	full outer join
                            |	(select org_id, grade_id,system_id, pad_class,subject_id,
                            |	            count(1) sc_number_all,
                            |				count(case when ext_id=0 and resource_tag=3 then resource_id end) sc_dxa_number,
                            |			    count(case when ext_id=0 and resource_tag=2 then resource_id end) sc_ja_number,
                            |				count(case when ext_id=0 and resource_tag=1 then resource_id end) sc_kj_number,
                            |				count(case when  resource_tag not in (1,2,3)  then resource_id end) sc_other_number,
                            |				time
                            |				from test.base_teacher_upload
                            |				group BY time,org_id,subject_id,grade_id,pad_class,system_id) upl on pub.time=upl.time and pub.org_id=upl.org_id and
                            |				pub.subject_id=upl.subject_id and pub.grade_id=upl.grade_id and pub.pad_class=upl.pad_class and pub.system_id=upl.system_id
                            |				) h) h1 """.stripMargin
    sql(str_create_tpust)
   /*班级使用详情*/
    val str_drop_cud=""" drop table test.class_use_details """
    sql(str_drop_cud)
    val str_create_cud=s""" create table test.class_use_details as
                          | select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          | (select distinct h.* from
                          | (select distinct  case when ans.org_id is not null then  ans.org_id
                          |                       when bro.org_id is not null then  bro.org_id
                          |					   else pub.org_id end org_id,
                          |			0 grade_id,
                          |			case when ans.class_id is not null then  ans.class_id
                          |                 when bro.class_id is not null then  bro.class_id
                          |			     else pub.class_id end class_id,
                          |			9999 is_pad,2 public_private,
                          |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number,
                          |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                          |			case when ans.sk_answer_num is not null then  ans.sk_answer_num else 0 end sk_answer_num,
                          |			case when ans.yx_answer_num is not null then  ans.yx_answer_num else 0 end yx_answer_num,
                          |			case when ans.zy_answer_num is not null then  ans.zy_answer_num else 0 end zy_answer_num,
                          |			case when ans.cp_answer_num is not null then  ans.cp_answer_num else 0 end cp_answer_num,
                          |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                          |			case when ans.time is not null then  ans.time
                          |                 when bro.time is not null then  bro.time
                          |			     else pub.time end time
                          |	 from
                          |			(select org_id,0 grade_id,class_id,9999 pad_class,
                          |				count(question_id) answer_all_num,
                          |				count(case when exercise_source=1 then question_id end ) sk_answer_num,
                          |				count(case when exercise_source=7 then question_id end ) yx_answer_num,
                          |				count(case when exercise_source=6 then question_id end ) zy_answer_num,
                          |				count(case when exercise_source=8 then question_id end ) cp_answer_num,
                          |				time from test.school_student_answer
                          |				group by time,org_id,class_id
                          |			) ans
                          |			full outer join
                          |			(select org_id,0 grade_id,class_id,9999 pad_class,
                          |				count(system_id) browse_all_num,
                          |				time from test.school_student_browse
                          |				group by time,org_id,class_id
                          |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.class_id=bro.class_id
                          |			full outer join
                          |			(
                          |			select org_id,0 grade_id,class_id,9999 pad_class,
                          |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                          |				time
                          |				from test.base_teacher_publish
                          |				group by time,org_id,class_id
                          |			) pub on pub.time=ans.time and pub.org_id=ans.org_id and pub.class_id=ans.class_id
                          |	union all
                          |	select distinct  case when ans.org_id is not null then  ans.org_id
                          |                       when bro.org_id is not null then  bro.org_id
                          |					   else pub.org_id end org_id,
                          |			0 grade_id,
                          |			case when ans.class_id is not null then  ans.class_id
                          |                 when bro.class_id is not null then  bro.class_id
                          |			     else pub.class_id end class_id,
                          |			case when ans.pad_class is not null then  ans.pad_class
                          |                 when bro.pad_class is not null then  bro.pad_class
                          |			     else pub.pad_class end  is_pad,2 public_private,
                          |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number,
                          |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                          |			case when ans.sk_answer_num is not null then  ans.sk_answer_num else 0 end sk_answer_num,
                          |			case when ans.yx_answer_num is not null then  ans.yx_answer_num else 0 end yx_answer_num,
                          |			case when ans.zy_answer_num is not null then  ans.zy_answer_num else 0 end zy_answer_num,
                          |			case when ans.cp_answer_num is not null then  ans.cp_answer_num else 0 end cp_answer_num,
                          |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                          |			case when ans.time is not null then  ans.time
                          |                 when bro.time is not null then  bro.time
                          |			     else pub.time end time
                          |	 from
                          |			(select org_id,0 grade_id,class_id,pad_class,
                          |				count(question_id) answer_all_num,
                          |				count(case when exercise_source=1 then question_id end ) sk_answer_num,
                          |				count(case when exercise_source=7 then question_id end ) yx_answer_num,
                          |				count(case when exercise_source=6 then question_id end ) zy_answer_num,
                          |				count(case when exercise_source=8 then question_id end ) cp_answer_num,
                          |				time from test.school_student_answer
                          |				group by time,org_id,class_id,pad_class
                          |			) ans
                          |			full outer join
                          |			(select org_id,0 grade_id,class_id,pad_class,
                          |				count(system_id) browse_all_num,
                          |				time from test.school_student_browse
                          |				group by time,org_id,class_id,pad_class
                          |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.class_id=bro.class_id and ans.pad_class=bro.pad_class
                          |			full outer join
                          |			(
                          |			select org_id,0 grade_id,class_id, pad_class,
                          |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                          |				time
                          |				from test.base_teacher_publish
                          |				group by time,org_id,class_id,pad_class
                          |			) pub on pub.time=ans.time and pub.org_id=ans.org_id and pub.class_id=ans.class_id and ans.pad_class=pub.pad_class
                          |			union all
                          |	select distinct  case when ans.org_id is not null then  ans.org_id
                          |                       when bro.org_id is not null then  bro.org_id
                          |					   else pub.org_id end org_id,
                          |			case when ans.grade_id is not null then  ans.grade_id
                          |                 when bro.grade_id is not null then  bro.grade_id
                          |			     else pub.grade_id end  grade_id,
                          |			case when ans.class_id is not null then  ans.class_id
                          |                 when bro.class_id is not null then  bro.class_id
                          |			     else pub.class_id end class_id,
                          |			9999 is_pad,2 public_private,
                          |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number,
                          |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                          |			case when ans.sk_answer_num is not null then  ans.sk_answer_num else 0 end sk_answer_num,
                          |			case when ans.yx_answer_num is not null then  ans.yx_answer_num else 0 end yx_answer_num,
                          |			case when ans.zy_answer_num is not null then  ans.zy_answer_num else 0 end zy_answer_num,
                          |			case when ans.cp_answer_num is not null then  ans.cp_answer_num else 0 end cp_answer_num,
                          |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                          |			case when ans.time is not null then  ans.time
                          |                 when bro.time is not null then  bro.time
                          |			     else pub.time end time
                          |	 from
                          |			(select org_id,grade_id,class_id,9999 pad_class,
                          |				count(question_id) answer_all_num,
                          |				count(case when exercise_source=1 then question_id end ) sk_answer_num,
                          |				count(case when exercise_source=7 then question_id end ) yx_answer_num,
                          |				count(case when exercise_source=6 then question_id end ) zy_answer_num,
                          |				count(case when exercise_source=8 then question_id end ) cp_answer_num,
                          |				time from test.school_student_answer
                          |				group by time,org_id,grade_id,class_id
                          |			) ans
                          |			full outer join
                          |			(select org_id,grade_id,class_id,9999 pad_class,
                          |				count(system_id) browse_all_num,
                          |				time from test.school_student_browse
                          |				group by time,org_id,grade_id,class_id
                          |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.class_id=bro.class_id and ans.grade_id=bro.grade_id
                          |			full outer join
                          |			(
                          |			select org_id,grade_id,class_id,9999 pad_class,
                          |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                          |				time
                          |				from test.base_teacher_publish
                          |				group by time,org_id,grade_id,class_id
                          |			) pub on pub.time=ans.time and pub.org_id=ans.org_id and pub.class_id=ans.class_id and ans.grade_id=pub.grade_id
                          |			union all
                          |	select distinct  case when ans.org_id is not null then  ans.org_id
                          |                       when bro.org_id is not null then  bro.org_id
                          |					   else pub.org_id end org_id,
                          |			case when ans.grade_id is not null then  ans.grade_id
                          |                 when bro.grade_id is not null then  bro.grade_id
                          |			     else pub.grade_id end  grade_id,
                          |			case when ans.class_id is not null then  ans.class_id
                          |                 when bro.class_id is not null then  bro.class_id
                          |			     else pub.class_id end class_id,
                          |			case when ans.pad_class is not null then  ans.pad_class
                          |                 when bro.pad_class is not null then  bro.pad_class
                          |			     else pub.pad_class end is_pad,
                          |				 2 public_private,
                          |			case when pub.fb_number_all is not null then  pub.fb_number_all else 0 end fb_number,
                          |			case when ans.answer_all_num is not null then  ans.answer_all_num else 0 end answer_num,
                          |			case when ans.sk_answer_num is not null then  ans.sk_answer_num else 0 end sk_answer_num,
                          |			case when ans.yx_answer_num is not null then  ans.yx_answer_num else 0 end yx_answer_num,
                          |			case when ans.zy_answer_num is not null then  ans.zy_answer_num else 0 end zy_answer_num,
                          |			case when ans.cp_answer_num is not null then  ans.cp_answer_num else 0 end cp_answer_num,
                          |			case when bro.browse_all_num is not null then  bro.browse_all_num else 0 end browse_num,
                          |			case when ans.time is not null then  ans.time
                          |                 when bro.time is not null then  bro.time
                          |			     else pub.time end time
                          |	 from
                          |			(select org_id,grade_id,class_id, pad_class,
                          |				count(question_id) answer_all_num,
                          |				count(case when exercise_source=1 then question_id end ) sk_answer_num,
                          |				count(case when exercise_source=7 then question_id end ) yx_answer_num,
                          |				count(case when exercise_source=6 then question_id end ) zy_answer_num,
                          |				count(case when exercise_source=8 then question_id end ) cp_answer_num,
                          |				time from test.school_student_answer
                          |				group by time,org_id,grade_id,class_id,pad_class
                          |			) ans
                          |			full outer join
                          |			(select org_id,grade_id,class_id, pad_class,
                          |				count(system_id) browse_all_num,
                          |				time from test.school_student_browse
                          |				group by time,org_id,grade_id,class_id,pad_class
                          |			) bro on ans.time=bro.time  and  ans.org_id=bro.org_id and ans.class_id=bro.class_id and ans.grade_id=bro.grade_id and ans.pad_class=bro.pad_class
                          |			full outer join
                          |			(
                          |			select org_id,grade_id,class_id, pad_class,
                          |				sum(CASE WHEN pu_type in (1,6,7,8) then 1   else 0 END) fb_number_all,
                          |				time
                          |				from test.base_teacher_publish
                          |				group by time,org_id,grade_id,class_id,pad_class
                          |			) pub on pub.time=ans.time and pub.org_id=ans.org_id and pub.class_id=ans.class_id and ans.grade_id=pub.grade_id and pub.pad_class=ans.pad_class
                          |			) h ) h1 """.stripMargin
    sql(str_create_cud)
    sc_1.stop()
  }
}
