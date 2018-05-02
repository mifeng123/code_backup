package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/11/6.
  */
object DataQuality {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("SchoolDataQuality")
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
    /*val DAY6AGO = args(4).toString*/
    val str_drop_swq=""" drop table test.school_weekly_question """.stripMargin
    sql(str_drop_swq)
    val str_create_swq=s"""  create table test.school_weekly_question as
                          |select eu.org_id,es.org_type,ese.student_id,ese.question_id,ese.ret_num,ese.subject_ex subject_id,eq.difficulty difficulty,
                          |ese.exercise_source  exercise_source ,ec.grade_id  grade_id,ec.id class_id,ec.pad_class pad_class,
                          |to_date(ese.submit_time) time from student_answer.student_answer_data ese
                          |join neworiental_v3.entity_question eq on eq.id=ese.question_id
                          |join neworiental_user.entity_user eu  on eu.system_id=ese.student_id
                          |join
                          |(
                          |select org_id,case when private=0 then 2 else 4 end org_type from neworiental_logdata.entity_school where enable=1 and private=0
                          |union all
                          |select id  org_id,2 org_type from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                          |) es  on es.org_id=eu.org_id
                          |join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                          |join neworiental_user.entity_classes ec on ec.id=lct.class_id
                          |where  eu.type=2 and lct.checked_status=2 and eu.status=1
                          |and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                          |and ese.ret_num>0 and ese.submit_time is not null and ese.subject_ex is not null and ese.subject_ex>0
                          |and ese.submit_time != 'null' and ese.submit_time != 'NULL' and ese.date>='${DATE}' and
                          |ese.date<'${DATENOW}' and ec.class_type!=3  and ec.class_type!=2  """.stripMargin
    sql(str_create_swq)


 /*   val str_drop_dqa="""  drop table  test.data_quality_autonomy """.stripMargin
    sql(str_drop_dqa)
    val str_create_dqa=s""" create table test.data_quality_autonomy as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(	select distinct h.org_id,h.grade_id,h.class_id,h.student_id system_id,2 public_private,h.subject_id,
                          |	h.right_num answer_right_num,h.all_num answer_all_num,
                          |	h.zz_right_num auto_right_num,h.zz_num auto_answer_num,
                          |	h.fz_right_num no_auto_right_num,h.fz_num no_auto_answer_num,
                          |	h.pad_class is_pad,h.time
                          |	from
                          |(	select org_id,0 grade_id,0 subject_id,class_id,student_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,class_id,student_id
                          |	union all
                          |	-----所有年级，pad
                          |	select org_id,0 grade_id,0 subject_id,class_id,student_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,pad_class,class_id,student_id
                          |	union all
                          |	--各个年级
                          |	select org_id,grade_id,0 subject_id,class_id,student_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,grade_id,class_id,student_id
                          |	union all
                          |	--各个年级，pad
                          |	select org_id,grade_id,0 subject_id,class_id,student_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,grade_id,pad_class,class_id,student_id
                          |	union all
                          |	---各个年级学科
                          |	select org_id,grade_id,subject_id,class_id,student_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,grade_id,subject_id,class_id,student_id
                          |	union all
                          |	---各个年级学科，pad
                          |	select org_id,grade_id,subject_id,class_id,student_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when exercise_source not in (1,6,7,8) then 1 else 0 end) zz_num,
                          |	sum(case when exercise_source not in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source not in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) zz_right_num,
                          |	sum(case when exercise_source  in (1,6,7,8) then 1 else 0 end) fz_num,
                          |	sum(case when exercise_source  in (1,6,7,8) and ret_num=1 then 1
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,grade_id,subject_id,pad_class,class_id,student_id
                          |) h) h1 """.stripMargin
    sql(str_create_dqa)*/

    val str_drop_dqc=""" drop table  test.data_quality_class """.stripMargin
    sql(str_drop_dqc)
    val str_create_dqc=s""" create table test.data_quality_class as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(select distinct h.org_id,h.grade_id,h.class_id,h.org_type public_private,h.subject_id,h.all_num answer_num,h.right_num,h.pad_class is_pad,h.time from
                          |(	select org_id,org_type,0 grade_id,0 subject_id,class_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,class_id
                          |	union all
                          |	-----所有年级，pad
                          |	select org_id,org_type,0 grade_id,0 subject_id,class_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,pad_class,class_id
                          |	union all
                          |	--各个年级
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,class_id
                          |	union all
                          |	--各个年级，pad
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,pad_class,class_id
                          |	union all
                          |	---各个年级学科
                          |	select org_id,org_type,grade_id,subject_id,class_id,9999 pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,class_id
                          |	union all
                          |	---各个年级学科，pad
                          |	select org_id,org_type,grade_id,subject_id,class_id,pad_class,
                          |	count(question_id) all_num,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,pad_class,class_id
                          |) h) h1 """.stripMargin
    sql(str_create_dqc)

    val str_drop_dqd=""" drop table test.data_quality_difficulty """.stripMargin
    sql(str_drop_dqd)
    val str_create_dqd=s""" create table test.data_quality_difficulty as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(   select distinct h.org_id,h.grade_id,h.class_id,h.org_type public_private,h.subject_id,
                          |	h.e_right_num easy_right_num,h.e_all_num easy_answer_num,h.m_right_num middle_right_num,h.m_all_num middle_answer_num,
                          |	h.h_right_num hard_right_num,h.h_all_num hard_answer_num,h.hs_right_num hards_right_num,h.hs_all_num hards_answer_num,
                          |	h.pad_class is_pad,h.time
                          |	from
                          |(	select org_id,org_type,0 grade_id,0 subject_id,class_id,9999 pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,class_id
                          |	union all
                          |	-----所有年级，pad
                          |	select org_id,org_type,0 grade_id,0 subject_id,class_id,pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,pad_class,class_id
                          |	union all
                          |	--各个年级
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,9999 pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_type,org_id,grade_id,class_id
                          |	union all
                          |	--各个年级，pad
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,pad_class,class_id
                          |	union all
                          |	---各个年级学科
                          |	select org_id,org_type,grade_id,subject_id,class_id,9999 pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,class_id
                          |	union all
                          |	---各个年级学科，pad
                          |	select org_id,org_type,grade_id,subject_id,class_id,pad_class,
                          |	sum(case when difficulty=1 then 1 else 0 end)  e_all_num,
                          |	sum(case when difficulty=1 and ret_num=1 then 1
                          |	         when difficulty=1 and ret_num=5 then 0.5 else 0 end) e_right_num,
                          |
                          |	sum(case when difficulty=2 then 1 else 0 end)  m_all_num,
                          |	sum(case when difficulty=2 and ret_num=1 then 1
                          |	         when difficulty=2 and ret_num=5 then 0.5 else 0 end) m_right_num,
                          |
                          |	sum(case when difficulty=3 then 1 else 0 end)  h_all_num,
                          |	sum(case when difficulty=3 and ret_num=1 then 1
                          |	         when difficulty=3 and ret_num=5 then 0.5 else 0 end) h_right_num,
                          |
                          |	sum(case when difficulty=4 then 1 else 0 end)  hs_all_num,
                          |	sum(case when difficulty=4 and ret_num=1 then 1
                          |	         when difficulty=4 and ret_num=5 then 0.5 else 0 end) hs_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,pad_class,class_id
                          |) h) h1 """.stripMargin
    sql(str_create_dqd)

    val str_drop_dqs=""" drop table test.data_quality_scene """.stripMargin
    sql(str_drop_dqs)
    val str_create_dqs=s""" create table test.data_quality_scene as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(	select distinct h.org_id,h.grade_id,h.class_id,h.org_type public_private,h.subject_id,
                          |	h.sk_right_num,h.sk_all_num sk_answer_num,h.yx_right_num,h.yx_all_num yx_answer_num,
                          |	h.zy_right_num,h.zy_all_num zy_answer_num,h.cp_right_num,h.cp_all_num cp_answer_num,
                          |	h.pad_class is_pad,
                          |	h.time from
                          |(	select org_id,org_type,0 grade_id,0 subject_id,class_id,9999 pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |	         when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,class_id
                          |	union all
                          |	-----所有年级，pad
                          |	select org_id,org_type,0 grade_id,0 subject_id,class_id,pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |	         when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,pad_class,class_id
                          |	union all
                          |	--各个年级
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,9999 pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |	         when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,class_id
                          |	union all
                          |	--各个年级，pad
                          |	select org_id,org_type,grade_id,0 subject_id,class_id,pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |
                          |  when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,pad_class,class_id
                          |	union all
                          |	---各个年级学科
                          |	select org_id,org_type,grade_id,subject_id,class_id,9999 pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |	         when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,class_id
                          |	union all
                          |	---各个年级学科，pad
                          |	select org_id,org_type,grade_id,subject_id,class_id,pad_class,
                          |	sum(case when exercise_source=1	 then 1 else 0 end)  sk_all_num,
                          |	sum(case when exercise_source=1	 and ret_num=1 then 1
                          |	         when exercise_source=1	 and ret_num=5 then 0.5 else 0 end) sk_right_num,
                          |
                          |	sum(case when exercise_source=6 then 1 else 0 end)  zy_all_num,
                          |	sum(case when exercise_source=6 and ret_num=1 then 1
                          |	         when exercise_source=6 and ret_num=5 then 0.5 else 0 end) zy_right_num,
                          |
                          |	sum(case when exercise_source=7 then 1 else 0 end)  cp_all_num,
                          |	sum(case when exercise_source=7 and ret_num=1 then 1
                          |	         when exercise_source=7 and ret_num=5 then 0.5 else 0 end) cp_right_num,
                          |
                          |	sum(case when exercise_source=8 then 1 else 0 end)  yx_all_num,
                          |	sum(case when exercise_source=8 and ret_num=1 then 1
                          |	         when exercise_source=8 and ret_num=5 then 0.5 else 0 end) yx_right_num,
                          |	time from test.school_weekly_question
                          |	group by time,org_id,org_type,grade_id,subject_id,pad_class,class_id
                          |) h) h1 """.stripMargin
    sql(str_create_dqs)
    sc_1.stop()

  }
}
