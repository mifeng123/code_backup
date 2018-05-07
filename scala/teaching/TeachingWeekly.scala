package teaching

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ztf on 2017/10/24.
  */
object TeachingWeekly {
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
   /* val str_drop_sq=s""" drop table test.student_question """.stripMargin
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
                      |ese.submit_time<'${DATENOW}' """.stripMargin
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
                         |		where es.private=0 and es.enable=1 and eu.type=2 and eu.status=1  and eu.org_id=ec.org_id and  eu.org_type=ec.org_type """.stripMargin
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
                         |			and apu.day>='${DAY6AGO}' and  apu.day<='${DAY}' """.stripMargin
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
                          |	and eu.org_id=ec.org_id and eu.org_type=ec.org_type """.stripMargin
    sql(str_create_sau)*/
    /*最终数据*/
    val str_drop_tclg=s""" drop table test.teaching_condition_list_grade """.stripMargin
    sql(str_drop_tclg)
    val str_create_tclg=s""" create table test.teaching_condition_list_grade as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,h.org_id org_id,eg.id grade_id,h.stage_id stage_id,h.org_type public_private,'${DATE}' time from
                           |(select org_id,org_type,stage_id  from test.subject_stage group by org_id,org_type,stage_id) h
                           |join neworiental_v3.entity_grade eg on eg.stage_id=h.stage_id  """.stripMargin
    sql(str_create_tclg)
    val str_drop_tclw=s""" drop table test.teaching_condition_list_week """.stripMargin
    sql(str_drop_tclw)
    val str_create_tclw=s""" create table test.teaching_condition_list_week as
                           | select cast(concat('${DAY}',row_number() over()) as bigint) id,h.org_id org_id,year('${DATE6AGO}') leran_year, weekofyear('${DATE}') leran_week,'${DATE6AGO}'  monday,'${DATE}' sunday,
                           | h.org_type public_private,'${DATE}' time from
                           |(select org_id,org_type  from test.subject_stage group by org_id,org_type) h """.stripMargin
    sql(str_create_tclw)

    val str_drop_tous=s""" drop table test.teaching_overall_usage """.stripMargin
    sql(str_drop_tous)
    val str_create_tous=s""" create table test.teaching_overall_usage as select cast(concat('${DAY}',row_number() over()) as bigint) id,h.* from
                           | (select distinct  h_user.org_id org_id,h_user.grade_id grade_id,h_user.class_id clas_id,h_user.org_type public_private,
                           |        h_user.teacher_num teacher_num,case when h_tdaily.teacher_daily_num is not null then h_tdaily.teacher_daily_num else 0 end teacher_daily,
                           |		h_user.student_num student_num,case when h_sdaily.studnet_daily_num is not null then  h_sdaily.studnet_daily_num else 0 end student_daily_num,
                           |		h_user.class_num class_num,  case when h_cdaily.class_daily_num is not null then  h_cdaily.class_daily_num else 0 end class_daily,
                           |		case when h_teacher.fb_cp_number is not null then h_teacher.fb_cp_number else 0 end fb_cp_num,
                           |		case when h_teacher.fb_sk_number is not null then h_teacher.fb_sk_number else 0 end fb_sk_num,
                           |		case when h_teacher.fb_yx_number is not null then h_teacher.fb_yx_number else 0 end fb_yx_num,
                           |		case when h_teacher.fb_zy_number is not null then h_teacher.fb_zy_number else 0 end fb_zy_num,
                           |		case when (h_student.answer_num/h_user.student_num) is not null then (h_student.answer_num/h_user.student_num) else 0 end per_capita_answer,
                           |        year('${DATE6AGO}') leran_year, weekofyear('${DATE6AGO}') leran_week,h_user.pad_class is_pad,h_user.time
                           |	--学生,老师,班级数量
                           |    from test.user_summary h_user
                           |	--老师发布
                           |     left join test.teacher_publish_summary h_teacher on  h_user.org_id=h_teacher.org_id and h_user.grade_id=h_teacher.grade_id and h_user.class_id=h_teacher.class_id and h_user.pad_class=h_teacher.pad_class and h_teacher.time=h_user.time
                           |	--学生答题
                           |	 left join  test.student_question_summary h_student on h_user.org_id=h_student.org_id and h_user.grade_id=h_student.grade_id and h_user.class_id=h_student.class_id and h_user.pad_class=h_student.pad_class and h_student.time=h_user.time
                           |	--学生活跃
                           |	left join  test.student_daily_summary h_sdaily on h_user.org_id=h_sdaily.org_id and h_user.grade_id=h_sdaily.grade_id and h_user.class_id=h_sdaily.class_id and h_user.pad_class=h_sdaily.pad_class and h_sdaily.time=h_user.time
                           |	--老师活跃
                           |	 left join test.teacher_daily_summary h_tdaily on h_user.org_id=h_tdaily.org_id and h_user.grade_id=h_tdaily.grade_id and h_user.class_id=h_tdaily.class_id and h_user.pad_class=h_tdaily.pad_class and h_tdaily.time=h_user.time
                           |	--班级活跃
                           |	 left join test.class_daily_summary h_cdaily on  h_user.org_id=h_cdaily.org_id and h_user.grade_id=h_cdaily.grade_id and h_user.class_id=h_cdaily.class_id and h_user.pad_class=h_cdaily.pad_class and h_cdaily.time=h_user.time) h """.stripMargin
    sql(str_create_tous)

    val str_drop_tsad=s""" drop table test.teaching_subject_answer_distribution """.stripMargin
    sql(str_drop_tsad)
    val str_create_tsad=s""" create table test.teaching_subject_answer_distribution as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id , h1.* from
                           |(select distinct   h.org_id org_id,h.org_type public_private,h.grade_id grade_id,h.subject_name subject_name,
                           |h.as_sk_num as_sk_num,h.as_sk_number as_sk_number,h.as_zy_num  as_zy_num,h.as_zy_number as_zy_number,h.as_cp_num  as_cp_num,
                           |h.as_cp_number as_cp_number,h.as_yx_num as_yx_num,h.as_yx_number  as_yx_number,
                           |year('${DATE6AGO}') leran_year, weekofyear('${DATE6AGO}') leran_week,h.pad_class is_pad, '${DATE}' time
                           | from
                           |	(select
                           |	distinct  s.org_id org_id,s.org_type org_type,0 grade_id,s.subject_name subject_name,
                           |	 case when st.cp_answer_num is not null then st.cp_answer_num else 0 end as_cp_num,
                           |	 case when st.cp_user_num is not null then st.cp_user_num else 0 end as_cp_number,
                           |	 case when st.sk_answer_num is not null then st.sk_answer_num else 0 end as_sk_num,
                           |	 case when st.sk_user_num is not null then st.sk_user_num else 0 end as_sk_number,
                           |	 case when st.yx_answer_num is not null then st.yx_answer_num else 0 end as_yx_num,
                           |	 case when st.yx_user_num is not null then st.yx_user_num else 0 end as_yx_number,
                           |	 case when st.zy_answer_num is not null then st.zy_answer_num else 0 end as_zy_num,
                           |	 case when st.zy_user_num is not null then st.zy_user_num else 0 end as_zy_number,
                           |	 9999 pad_class
                           |	  from test.subject_stage s
                           |	left join
                           |	(select * from test.student_subject_scence_summary where grade_id=0 and pad_class=9999
                           |	) st on st.org_id=s.org_id and s.subject_name=st.subject_name
                           |	union all
                           |	select
                           |	distinct  s.org_id org_id,s.org_type org_type,0 grade_id,s.subject_name subject_name,
                           |	 case when st.cp_answer_num is not null then st.cp_answer_num else 0 end as_cp_num,
                           |	 case when st.cp_user_num is not null then st.cp_user_num else 0 end as_cp_number,
                           |	 case when st.sk_answer_num is not null then st.sk_answer_num else 0 end as_sk_num,
                           |	 case when st.sk_user_num is not null then st.sk_user_num else 0 end as_sk_number,
                           |	 case when st.yx_answer_num is not null then st.yx_answer_num else 0 end as_yx_num,
                           |	 case when st.yx_user_num is not null then st.yx_user_num else 0 end as_yx_number,
                           |	 case when st.zy_answer_num is not null then st.zy_answer_num else 0 end as_zy_num,
                           |	 case when st.zy_user_num is not null then st.zy_user_num else 0 end as_zy_number,
                           |	 case when st.pad_class is not null then st.pad_class else 0 end pad_class
                           |	  from test.subject_stage s
                           |	left join
                           |	(select * from test.student_subject_scence_summary where grade_id=0 and pad_class!=9999
                           |	) st on st.org_id=s.org_id and s.subject_name=st.subject_name
                           |	union all
                           |	select
                           |	distinct  s.org_id org_id,s.org_type org_type,case when st.grade_id is not null then st.grade_id else 9999 end grade_id,
                           |	 s.subject_name subject_name,
                           |	 case when st.cp_answer_num is not null then st.cp_answer_num else 0 end as_cp_num,
                           |	 case when st.cp_user_num is not null then st.cp_user_num else 0 end as_cp_number,
                           |	 case when st.sk_answer_num is not null then st.sk_answer_num else 0 end as_sk_num,
                           |	 case when st.sk_user_num is not null then st.sk_user_num else 0 end as_sk_number,
                           |	 case when st.yx_answer_num is not null then st.yx_answer_num else 0 end as_yx_num,
                           |	 case when st.yx_user_num is not null then st.yx_user_num else 0 end as_yx_number,
                           |	 case when st.zy_answer_num is not null then st.zy_answer_num else 0 end as_zy_num,
                           |	 case when st.zy_user_num is not null then st.zy_user_num else 0 end as_zy_number,
                           |	 9999 pad_class
                           |	  from test.subject_stage s
                           |	left join
                           |	(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |				   when grade_id in(7,8,9) then 1
                           |				   when grade_id in(10,11,12) then 2 else 0 end stage_id from test.student_subject_scence_summary where grade_id!=0 and pad_class=9999
                           |	) st on st.org_id=s.org_id and s.subject_name=st.subject_name and st.stage_id=s.stage_id
                           |	union all
                           |	select
                           |	distinct  s.org_id org_id,s.org_type org_type,case when st.grade_id is not null then st.grade_id else 9999 end grade_id,
                           |	 s.subject_name subject_name,
                           |	 case when st.cp_answer_num is not null then st.cp_answer_num else 0 end as_cp_num,
                           |	 case when st.cp_user_num is not null then st.cp_user_num else 0 end as_cp_number,
                           |	 case when st.sk_answer_num is not null then st.sk_answer_num else 0 end as_sk_num,
                           |	 case when st.sk_user_num is not null then st.sk_user_num else 0 end as_sk_number,
                           |	 case when st.yx_answer_num is not null then st.yx_answer_num else 0 end as_yx_num,
                           |	 case when st.yx_user_num is not null then st.yx_user_num else 0 end as_yx_number,
                           |	 case when st.zy_answer_num is not null then st.zy_answer_num else 0 end as_zy_num,
                           |	 case when st.zy_user_num is not null then st.zy_user_num else 0 end as_zy_number,
                           |	 case when st.pad_class is not null then st.pad_class else 0 end pad_class
                           |	  from test.subject_stage s
                           |	left join
                           |	(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |				   when grade_id in(7,8,9) then 1
                           |				   when grade_id in(10,11,12) then 2 else 0 end stage_id from test.student_subject_scence_summary where grade_id!=0 and pad_class!=9999
                           |	) st on st.org_id=s.org_id and s.subject_name=st.subject_name and st.stage_id=s.stage_id
                           |) h )h1 """.stripMargin
    sql(str_create_tsad)

    val str_drop_tstu=s""" drop table test.teaching_subject_teacher_usage """.stripMargin
    sql(str_drop_tstu)
    val str_create_tstu=s""" create table test.teaching_subject_teacher_usage as
                           |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                           |(select distinct  h.org_id org_id,h.grade_id grade_id,h.org_type public_private,h.subject_name  subject_name,h.system_id system_id,h.login_num th_login_number,
                           |h.respackage_num th_zyb_number,h.sk_num fb_sk_number,h.cp_num fb_cp_number,h.yx_num fb_yx_number,h.zy_num fb_zy_number,
                           |h.manage_class_num th_glc_number,h.per_num per_capita_answer,year('${DATE6AGO}') leran_year, weekofyear('${DATE6AGO}') leran_week,h.pad_class is_pad,'${DATE}' time
                           |  from
                           |(select distinct ss.org_id,ss.org_type,0 grade_id,ss.system_id,9999 pad_class,ss.subject_name,
                           |	case when tl.login_num is not null then tl.login_num else 0 end login_num,
                           |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num,
                           |    case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                           |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                           |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                           |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                           |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                           |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                           |    from
                           |(select * from test.user_type_num where type=1
                           |) ss
                           |left join
                           |--老师登录天数
                           |(select * from test.teacher_login where grade_id=0 and pad_class=9999
                           |) tl on tl.org_id=ss.org_id and tl.subject_name=ss.subject_name and tl.system_id=ss.system_id
                           |left join
                           |--资源包创建
                           |(select * from test.teacher_subject_respackage where grade_id=0 and pad_class=9999
                           |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.system_id=ss.system_id
                           |left join
                           |--学科下的发布
                           |(select * from test.teacher_subject_publish_summary  where grade_id=0 and pad_class=9999
                           |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.system_id=ss.system_id
                           |left join
                           |--管理班级数,学生人均答题量
                           |(select * from test.student_subject_answer_summary where grade_id=0 and pad_class=9999
                           |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.system_id=ss.system_id
                           |
                           |union all
                           |select distinct ss.org_id,ss.org_type,0 grade_id ,ss.system_id,
                           |	case when sr.pad_class is not null then sr.pad_class else 0 end pad_class,ss.subject_name,
                           |	case when tl.login_num is not null then tl.login_num else 0 end login_num,
                           |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num,
                           |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                           |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                           |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                           |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                           |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                           |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                           |    from
                           |(select * from test.user_type_num where type=1
                           |) ss
                           |left join
                           |--老师登录天数
                           |(select * from test.teacher_login where grade_id=0 and pad_class!=9999
                           |) tl on tl.org_id=ss.org_id and tl.subject_name=ss.subject_name and tl.system_id=ss.system_id
                           |left join
                           |--资源包创建
                           |(select * from test.teacher_subject_respackage where grade_id=0 and pad_class!=9999
                           |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.system_id=ss.system_id
                           |left join
                           |--学科下的发布
                           |(select * from test.teacher_subject_publish_summary  where grade_id=0 and pad_class!=9999
                           |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.system_id=ss.system_id
                           |left join
                           |--管理班级数,学生人均答题量
                           |(select * from test.student_subject_answer_summary where grade_id=0 and pad_class!=9999
                           |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.system_id=ss.system_id
                           |union all
                           |select distinct ss.org_id,ss.org_type,
                           |    case when ss.grade_id is not null then ss.grade_id else 9999 end grade_id,
                           |	ss.system_id system_id,
                           |	9999 pad_class,ss.subject_name,
                           |	case when tl.login_num is not null then tl.login_num else 0 end login_num,
                           |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num ,
                           |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                           |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                           |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                           |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                           |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                           |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                           |    from
                           |(
                           |select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id  from test.user_type_num where type=1
                           |) ss
                           |left join
                           |--老师登录天数
                           |(select * from test.teacher_login where grade_id!=0 and pad_class=9999
                           |) tl on tl.org_id=ss.org_id and tl.subject_name=ss.subject_name and tl.system_id=ss.system_id
                           |left join
                           |--资源包创建
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                           |			   from test.teacher_subject_respackage where grade_id!=0 and pad_class=9999
                           |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.stage_id=ss.stage_id
                           |		and sr.system_id=ss.system_id
                           |left join
                           |--学科下的发布
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                           |			   from test.teacher_subject_publish_summary  where grade_id!=0 and pad_class=9999
                           |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.stage_id=ss.stage_id
                           |		and tsp.system_id=ss.system_id
                           |left join
                           |--管理班级数,学生人均答题量
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                           |			   from test.student_subject_answer_summary where grade_id!=0 and pad_class=9999
                           |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.stage_id=ss.stage_id
                           |		and sps.system_id=ss.system_id
                           |union all
                           |select distinct ss.org_id,ss.org_type,
                           |  case when ss.grade_id is not null then ss.grade_id else 9999 end grade_id,
                           |	ss.system_id system_id,
                           |	case when sr.pad_class is not null then sr.pad_class else 0 end pad_class, ss.subject_name,
                           |	case when tl.login_num is not null then tl.login_num else 0 end login_num,
                           |  case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num ,
                           |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                           |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                           |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                           |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                           |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                           |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                           |    from
                           |(
                           |select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id  from test.user_type_num where type=1
                           |) ss
                           |left join
                           |--老师登录天数
                           |(select * from test.teacher_login where grade_id!=0 and pad_class!=9999
                           |) tl on tl.org_id=ss.org_id and tl.subject_name=ss.subject_name and tl.system_id=ss.system_id
                           |left join
                           |--资源包创建
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			         when grade_id in(10,11,12) then 2 else 0 end stage_id
                           | from test.teacher_subject_respackage where grade_id!=0 and pad_class!=9999
                           |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.stage_id=ss.stage_id
                           |and sr.system_id=ss.system_id
                           |left join
                           |--学科下的发布
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                           |			   from test.teacher_subject_publish_summary  where grade_id!=0 and pad_class!=9999
                           |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.stage_id=ss.stage_id
                           |and tsp.system_id=ss.system_id
                           |left join
                           |--管理班级数,学生人均答题量
                           |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                           |               when grade_id in(7,8,9) then 1
                           |			         when grade_id in(10,11,12) then 2 else 0 end stage_id
                           |			   from test.student_subject_answer_summary where grade_id!=0 and pad_class!=9999
                           |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.stage_id=ss.stage_id
                           |and sps.system_id=ss.system_id ) h ) h1
                           |
 |  """.stripMargin
    sql(str_create_tstu)


       val str_drop_tsu=s""" drop table test.teaching_subject_usage """.stripMargin
       sql(str_drop_tsu)
       val str_create_tsu=s""" create table test.teaching_subject_usage as
                             |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                             |(select  distinct  h.org_id  org_id,h.grade_id grade_id,h.org_type public_private,h.subject_name subject_name,
                             |h.respackage_num th_zyb_number,h.sk_num fb_sk_number,h.cp_num fb_cp_number,h.yx_num fb_yx_number,h.zy_num fb_zy_number,
                             |h.manage_class_num th_glc_number,h.per_num per_capita_answer,year('${DATE6AGO}') leran_year, weekofyear('${DATE6AGO}') leran_week,h.pad_class is_pad,'${DATE}' time from
                             |(
                             |select distinct ss.org_id,ss.org_type,0 grade_id ,9999 pad_class,ss.subject_name,
                             |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num,
                             |    case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                             |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                             |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                             |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                             |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                             |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                             |    from test.subject_stage ss
                             |left join
                             |--资源包创建
                             |(select * from test.subject_respackage where grade_id=0 and pad_class=9999
                             |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name
                             |left join
                             |--学科下的发布
                             |(select * from test.subject_publish_summary  where grade_id=0 and pad_class=9999
                             |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name
                             |left join
                             |--管理班级数,学生人均答题量
                             |(select * from test.subject_answer_summary where grade_id=0 and pad_class=9999
                             |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name
                             |union all
                             |select distinct ss.org_id,ss.org_type,0 grade_id ,
                             |case when sr.pad_class is not null then sr.pad_class else 0 end pad_class,ss.subject_name,
                             |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num,
                             |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                             |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                             |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                             |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                             |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                             |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                             |    from test.subject_stage ss
                             |left join
                             |--资源包创建
                             |(select * from test.subject_respackage where grade_id=0 and pad_class!=9999
                             |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name
                             |left join
                             |--学科下的发布
                             |(select * from test.subject_publish_summary  where grade_id=0 and pad_class!=9999
                             |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name
                             |left join
                             |--管理班级数,学生人均答题量
                             |(select * from test.subject_answer_summary where grade_id=0 and pad_class!=9999
                             |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name
                             |union all
                             |select distinct ss.org_id,ss.org_type,
                             |    case when sr.grade_id is not null then sr.grade_id else 9999 end grade_id,9999 pad_class,ss.subject_name,
                             |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num ,
                             |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                             |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                             |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end cp_num,
                             |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end yx_num,
                             |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                             |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                             |    from test.subject_stage ss
                             |left join
                             |--资源包创建
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             |			   from test.subject_respackage where grade_id!=0 and pad_class=9999
                             |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.stage_id=ss.stage_id
                             |left join
                             |--学科下的发布
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             |			   from test.subject_publish_summary  where grade_id!=0 and pad_class=9999
                             |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.stage_id=ss.stage_id
                             |left join
                             |--管理班级数,学生人均答题量
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             |			   from test.subject_answer_summary where grade_id!=0 and pad_class=9999
                             |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.stage_id=ss.stage_id
                             |where  sr.grade_id=tsp.grade_id and  tsp.grade_id=sps.grade_id
                             |union all
                             |select distinct ss.org_id,ss.org_type,
                             |    case when sr.grade_id is not null then sr.grade_id else 9999 end grade_id,
                             |	case when sr.pad_class is not null then sr.pad_class else 0 end pad_class, ss.subject_name,
                             |    case when sr.respackage_num is not null then sr.respackage_num else 0 end  respackage_num ,
                             |	case when tsp.fb_sk_number	is not null then  tsp.fb_sk_number else 0 end  sk_num,
                             |	case when tsp.fb_zy_number  is not null then  tsp.fb_zy_number else 0 end  zy_num,
                             |	case when tsp.fb_cp_number  is not null then  tsp.fb_cp_number else 0 end  cp_num,
                             |	case when tsp.fb_yx_number  is not null then  tsp.fb_yx_number else 0 end  yx_num,
                             |	case when sps.manage_class_num is not null  then sps.manage_class_num else 0 end   manage_class_num,
                             |	case when (sps.subject_answer_num/sps.student_num) is not null then (sps.subject_answer_num/sps.student_num) else 0 end per_num
                             |    from test.subject_stage ss
                             |left join
                             |--资源包创建
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             | from test.subject_respackage where grade_id!=0 and pad_class!=9999
                             |) sr  on sr.org_id=ss.org_id and sr.subject_name=ss.subject_name and sr.stage_id=ss.stage_id
                             |left join
                             |--学科下的发布
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             |			   from test.subject_publish_summary  where grade_id!=0 and pad_class!=9999
                             |) tsp on tsp.org_id=ss.org_id and tsp.subject_name=ss.subject_name and tsp.stage_id=ss.stage_id
                             |left join
                             |--管理班级数,学生人均答题量
                             |(select *,case when grade_id in(1,2,3,4,5,6) then 3
                             |               when grade_id in(7,8,9) then 1
                             |			   when grade_id in(10,11,12) then 2 else 0 end stage_id
                             |			   from test.subject_answer_summary where grade_id!=0 and pad_class!=9999
                             |) sps on sps.org_id=ss.org_id and sps.subject_name=ss.subject_name and sps.stage_id=ss.stage_id
                             |where  sr.grade_id=tsp.grade_id and  tsp.grade_id=sps.grade_id
                             |) h  )h1 """.stripMargin
       sql(str_create_tsu)

       val str_drop_ttsu=s""" drop table test.teaching_teacher_student_usage  """.stripMargin
       sql(str_drop_ttsu)
       val str_create_ttsu=s"""  create table test.teaching_teacher_student_usage as
                             |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                             |(select    distinct  h.org_id,h.grade_id,h.org_type public_private,h.pad_class is_pad,h.kj_num,
                             |h.dxa_num,h.ja_num,h.sp_num,h.tp_num,h.wk_num,h.wd_num,h.yp_num,
                             |h.wk_app_num,h.xt_app_num,h.xiti_user_num,h.xta_num,h.xtr_num,h.xtl,
                             |h.zya_num,h.zyr_num,h.zyl,h.cpa_num,h.cpr_num,h.cpl,h.fxa_num,h.fxr_num,h.fxl,h.yxa_num,h.yxr_num,h.yxl,
                             |h.ska_num,h.skr_num,h.skl,year('${DATE6AGO}') leran_year, weekofyear('${DATE6AGO}') leran_week, cast(h.time as date) time
                             |from
                             |(select st.org_id,st.org_type,0 grade_id,9999 pad_class,
                             |case when tus.kj_num is not null then tus.kj_num else 0 end kj_num,
                             |case when tus.dxa_num is not null then tus.dxa_num else 0 end dxa_num,
                             |case when tus.ja_num is not null then tus.ja_num else 0 end ja_num,
                             |case when tus.sp_num is not null then tus.sp_num else 0 end sp_num,
                             |case when tus.tp_num is not null then tus.tp_num else 0 end tp_num,
                             |case when tus.wk_num is not null then tus.wk_num else 0 end wk_num,
                             |case when tus.wd_num is not null then tus.wd_num else 0 end wd_num,
                             |case when tus.yp_num is not null then tus.yp_num else 0 end yp_num,
                             |case when sas.wk_app_num is not null then sas.wk_app_num else 0 end wk_app_num,
                             |case when sas.xt_app_num is not null then sas.xt_app_num else 0 end xt_app_num,
                             |case when sss.xiti_user_num is not null then sss.xiti_user_num else 0 end xiti_user_num,
                             |case when sss.xta_num is not null then sss.xta_num else 0 end xta_num,
                             |case when sss.xtr_num is not null then sss.xtr_num else 0 end xtr_num,
                             |case when sss.xtl is not null then sss.xtl else 0 end xtl,
                             |case when sss.zya_num is not null then sss.zya_num else 0 end zya_num,
                             |case when sss.zyr_num is not null then sss.zyr_num else 0 end zyr_num,
                             |case when sss.zyl is not null then sss.zyl else 0 end zyl,
                             |case when sss.cpa_num is not null then sss.cpa_num else 0 end cpa_num,
                             |case when sss.cpr_num is not null then sss.cpr_num else 0 end cpr_num,
                             |case when sss.cpl is not null then sss.cpl else 0 end cpl,
                             |case when sss.fxa_num is not null then sss.fxa_num else 0 end fxa_num,
                             |case when sss.fxr_num is not null then sss.fxr_num else 0 end fxr_num,
                             |case when sss.fxl is not null then sss.fxl else 0 end fxl,
                             |case when sss.yxa_num is not null then sss.yxa_num else 0 end yxa_num,
                             |case when sss.yxr_num is not null then sss.yxr_num else 0 end yxr_num,
                             |case when sss.yxl is not null then sss.yxl else 0 end yxl,
                             |case when sss.ska_num is not null then sss.ska_num else 0 end ska_num,
                             |case when sss.skr_num is not null then sss.skr_num else 0 end skr_num,
                             |case when sss.skl is not null then sss.skl else 0 end skl,
                             |st.time
                             |from test.school_time st
                             |--老师上传
                             |left join (
                             |select * from test.teacher_upload_summary where grade_id=0 and pad_class=9999
                             |) tus on tus.org_id=st.org_id and tus.time=st.time
                             |--app用户
                             |left join
                             |(select * from  test.student_app_user_summary where grade_id=0 and pad_class=9999
                             |) sas on  sas.org_id=st.org_id and sas.time=st.time
                             |--学生各场景答题
                             |left join
                             |(select * from test.student_scence_summary where grade_id=0 and pad_class=9999
                             |) sss on  sss.org_id=st.org_id and sss.time=st.time
                             |union all
                             |select st.org_id,st.org_type,0 grade_id,
                             |case when sss.pad_class  is not null then  sss.pad_class else 0 end pad_class,
                             |case when tus.kj_num is not null then tus.kj_num else 0 end kj_num,
                             |case when tus.dxa_num is not null then tus.dxa_num else 0 end dxa_num,
                             |case when tus.ja_num is not null then tus.ja_num else 0 end ja_num,
                             |case when tus.sp_num is not null then tus.sp_num else 0 end sp_num,
                             |case when tus.tp_num is not null then tus.tp_num else 0 end tp_num,
                             |case when tus.wk_num is not null then tus.wk_num else 0 end wk_num,
                             |case when tus.wd_num is not null then tus.wd_num else 0 end wd_num,
                             |case when tus.yp_num is not null then tus.yp_num else 0 end yp_num,
                             |case when sas.wk_app_num is not null then sas.wk_app_num else 0 end wk_app_num,
                             |case when sas.xt_app_num is not null then sas.xt_app_num else 0 end xt_app_num,
                             |case when sss.xiti_user_num is not null then sss.xiti_user_num else 0 end xiti_user_num,
                             |case when sss.xta_num is not null then sss.xta_num else 0 end xta_num,
                             |case when sss.xtr_num is not null then sss.xtr_num else 0 end xtr_num,
                             |case when sss.xtl is not null then sss.xtl else 0 end xtl,
                             |case when sss.zya_num is not null then sss.zya_num else 0 end zya_num,
                             |case when sss.zyr_num is not null then sss.zyr_num else 0 end zyr_num,
                             |case when sss.zyl is not null then sss.zyl else 0 end zyl,
                             |case when sss.cpa_num is not null then sss.cpa_num else 0 end cpa_num,
                             |case when sss.cpr_num is not null then sss.cpr_num else 0 end cpr_num,
                             |case when sss.cpl is not null then sss.cpl else 0 end cpl,
                             |case when sss.fxa_num is not null then sss.fxa_num else 0 end fxa_num,
                             |case when sss.fxr_num is not null then sss.fxr_num else 0 end fxr_num,
                             |case when sss.fxl is not null then sss.fxl else 0 end fxl,
                             |case when sss.yxa_num is not null then sss.yxa_num else 0 end yxa_num,
                             |case when sss.yxr_num is not null then sss.yxr_num else 0 end yxr_num,
                             |case when sss.yxl is not null then sss.yxl else 0 end yxl,
                             |case when sss.ska_num is not null then sss.ska_num else 0 end ska_num,
                             |case when sss.skr_num is not null then sss.skr_num else 0 end skr_num,
                             |case when sss.skl is not null then sss.skl else 0 end skl,
                             |st.time
                             |from test.school_time st
                             |--老师上传
                             |left join (
                             |select * from test.teacher_upload_summary where grade_id=0 and pad_class!=9999
                             |) tus on tus.org_id=st.org_id and tus.time=st.time
                             |--app用户
                             |left join
                             |(select * from  test.student_app_user_summary where grade_id=0 and pad_class!=9999
                             |) sas on  sas.org_id=st.org_id and sas.time=st.time
                             |--学生各场景答题
                             |left join
                             |(select * from test.student_scence_summary where grade_id=0 and pad_class!=9999
                             |) sss on  sss.org_id=st.org_id and sss.time=st.time
                             |union all
                             |select st.org_id,st.org_type,
                             |case when sss.grade_id is not null then  sss.grade_id
                             |     when tus.grade_id is not null then  tus.grade_id
                             |     when sas.grade_id is not null then  sas.grade_id
                             |     else 9999 end grade_id,
                             |9999 pad_class,
                             |case when tus.kj_num is not null then tus.kj_num else 0 end kj_num,
                             |case when tus.dxa_num is not null then tus.dxa_num else 0 end dxa_num,
                             |case when tus.ja_num is not null then tus.ja_num else 0 end ja_num,
                             |case when tus.sp_num is not null then tus.sp_num else 0 end sp_num,
                             |case when tus.tp_num is not null then tus.tp_num else 0 end tp_num,
                             |case when tus.wk_num is not null then tus.wk_num else 0 end wk_num,
                             |case when tus.wd_num is not null then tus.wd_num else 0 end wd_num,
                             |case when tus.yp_num is not null then tus.yp_num else 0 end yp_num,
                             |case when sas.wk_app_num is not null then sas.wk_app_num else 0 end wk_app_num,
                             |case when sas.xt_app_num is not null then sas.xt_app_num else 0 end xt_app_num,
                             |case when sss.xiti_user_num is not null then sss.xiti_user_num else 0 end xiti_user_num,
                             |case when sss.xta_num is not null then sss.xta_num else 0 end xta_num,
                             |case when sss.xtr_num is not null then sss.xtr_num else 0 end xtr_num,
                             |case when sss.xtl is not null then sss.xtl else 0 end xtl,
                             |case when sss.zya_num is not null then sss.zya_num else 0 end zya_num,
                             |case when sss.zyr_num is not null then sss.zyr_num else 0 end zyr_num,
                             |case when sss.zyl is not null then sss.zyl else 0 end zyl,
                             |case when sss.cpa_num is not null then sss.cpa_num else 0 end cpa_num,
                             |case when sss.cpr_num is not null then sss.cpr_num else 0 end cpr_num,
                             |case when sss.cpl is not null then sss.cpl else 0 end cpl,
                             |case when sss.fxa_num is not null then sss.fxa_num else 0 end fxa_num,
                             |case when sss.fxr_num is not null then sss.fxr_num else 0 end fxr_num,
                             |case when sss.fxl is not null then sss.fxl else 0 end fxl,
                             |case when sss.yxa_num is not null then sss.yxa_num else 0 end yxa_num,
                             |case when sss.yxr_num is not null then sss.yxr_num else 0 end yxr_num,
                             |case when sss.yxl is not null then sss.yxl else 0 end yxl,
                             |case when sss.ska_num is not null then sss.ska_num else 0 end ska_num,
                             |case when sss.skr_num is not null then sss.skr_num else 0 end skr_num,
                             |case when sss.skl is not null then sss.skl else 0 end skl,
                             |st.time
                             |from test.school_time st
                             |--老师上传
                             |left join (
                             |select * from test.teacher_upload_summary where grade_id!=0 and pad_class=9999
                             |) tus on tus.org_id=st.org_id and tus.time=st.time
                             |--app用户
                             |left join
                             |(select * from  test.student_app_user_summary where grade_id!=0 and pad_class=9999
                             |) sas on  sas.org_id=st.org_id and sas.time=st.time
                             |--学生各场景答题
                             |left join
                             |(select * from test.student_scence_summary where grade_id!=0 and pad_class=9999
                             |) sss on  sss.org_id=st.org_id and sss.time=st.time
                             |union all
                             |select st.org_id,st.org_type,
                             |case when sss.grade_id is not null then  sss.grade_id
                             |     when tus.grade_id is not null then  tus.grade_id
                             |     when sas.grade_id is not null then  sas.grade_id
                             |     else 9999 end grade_id,
                             |case when sss.pad_class is not null then  sss.pad_class else 0 end pad_class,
                             |case when tus.kj_num is not null then tus.kj_num else 0 end kj_num,
                             |case when tus.dxa_num is not null then tus.dxa_num else 0 end dxa_num,
                             |case when tus.ja_num is not null then tus.ja_num else 0 end ja_num,
                             |case when tus.sp_num is not null then tus.sp_num else 0 end sp_num,
                             |case when tus.tp_num is not null then tus.tp_num else 0 end tp_num,
                             |case when tus.wk_num is not null then tus.wk_num else 0 end wk_num,
                             |case when tus.wd_num is not null then tus.wd_num else 0 end wd_num,
                             |case when tus.yp_num is not null then tus.yp_num else 0 end yp_num,
                             |case when sas.wk_app_num is not null then sas.wk_app_num else 0 end wk_app_num,
                             |case when sas.xt_app_num is not null then sas.xt_app_num else 0 end xt_app_num,
                             |case when sss.xiti_user_num is not null then sss.xiti_user_num else 0 end xiti_user_num,
                             |case when sss.xta_num is not null then sss.xta_num else 0 end xta_num,
                             |case when sss.xtr_num is not null then sss.xtr_num else 0 end xtr_num,
                             |case when sss.xtl is not null then sss.xtl else 0 end xtl,
                             |case when sss.zya_num is not null then sss.zya_num else 0 end zya_num,
                             |case when sss.zyr_num is not null then sss.zyr_num else 0 end zyr_num,
                             |case when sss.zyl is not null then sss.zyl else 0 end zyl,
                             |case when sss.cpa_num is not null then sss.cpa_num else 0 end cpa_num,
                             |case when sss.cpr_num is not null then sss.cpr_num else 0 end cpr_num,
                             |case when sss.cpl is not null then sss.cpl else 0 end cpl,
                             |case when sss.fxa_num is not null then sss.fxa_num else 0 end fxa_num,
                             |case when sss.fxr_num is not null then sss.fxr_num else 0 end fxr_num,
                             |case when sss.fxl is not null then sss.fxl else 0 end fxl,
                             |case when sss.yxa_num is not null then sss.yxa_num else 0 end yxa_num,
                             |case when sss.yxr_num is not null then sss.yxr_num else 0 end yxr_num,
                             |case when sss.yxl is not null then sss.yxl else 0 end yxl,
                             |case when sss.ska_num is not null then sss.ska_num else 0 end ska_num,
                             |case when sss.skr_num is not null then sss.skr_num else 0 end skr_num,
                             |case when sss.skl is not null then sss.skl else 0 end skl,
                             |st.time
                             |from test.school_time st
                             |--老师上传
                             |left join (
                             |select * from test.teacher_upload_summary where grade_id!=0 and pad_class!=9999
                             |) tus on tus.org_id=st.org_id and tus.time=st.time
                             |--app用户
                             |left join
                             |(select * from  test.student_app_user_summary where grade_id!=0 and pad_class!=9999
                             |) sas on  sas.org_id=st.org_id and sas.time=st.time
                             |--学生各场景答题
                             |left join
                             |(select * from test.student_scence_summary where grade_id!=0 and pad_class!=9999
                             |) sss on  sss.org_id=st.org_id and sss.time=st.time ) h )h1 """.stripMargin
       sql(str_create_ttsu)
    sc_1.stop()
  }
}
