package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/11/6.
  */
object StudentWeekly {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("SchoolStudentWeekly")
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
    val DAY = args(1).toString
    val DATE = args(2).toString
    /*val DAY6AGO = args(4).toString*/
    val str_drop_swqw=""" drop table test.school_weekly_question_week """.stripMargin
    sql(str_drop_swqw)
    val str_create_swqw=s""" create table test.school_weekly_question_week as
                           |select eu.org_id,ese.student_id,ese.question_id,ese.ret_num,ese.subject_ex subject_id,eq.difficulty difficulty,
                           |ese.exercise_source  exercise_source ,ec.grade_id  grade_id,ec.id class_id,ec.pad_class pad_class,
                           |to_date(ese.submit_time) time from student_answer.student_answer_data ese
                           |join neworiental_v3.entity_question eq on eq.id=ese.question_id
                           |join neworiental_user.entity_user eu  on eu.system_id=ese.student_id
                           |join (
                           |select org_id from neworiental_logdata.entity_school where enable=1 and private=0
                           |    union all
                           |select id  org_id from  neworiental_v3.entity_public_school where id in (80,132,135,684)
                           |) es  on es.org_id=eu.org_id
                           |join neworiental_user.l_class_student lct on lct.student_system_id=eu.system_id
                           |join neworiental_user.entity_classes ec on ec.id=lct.class_id
                           |where eu.type=2 and lct.checked_status=2 and eu.status=1
                           |and eu.org_id=ec.org_id and eu.org_type=ec.org_type
                           |and ese.ret_num>0 and ese.submit_time is not null and ese.subject_ex is not null and ese.subject_ex > 0
                           |and ese.submit_time != 'null' and ese.submit_time != 'NULL' and ese.date>='${DATE6AGO}' and
                           |ese.date<='${DATE}' and ec.class_type!=2 and ec.class_type!=3 """.stripMargin
    sql(str_create_swqw)

    val str_drop_sws=""" drop table test.student_weekly_survey """.stripMargin
    sql(str_drop_sws)
    val str_create_sws=s""" create table test.student_weekly_survey as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |	(select distinct h.org_id,h.grade_id,h.class_id,h.student_id system_id,
                          |	2 public_private,h.exercise_source pointer_type,h.subject_id,
                          |	h.right_num,h.wrong_num,h.unknown_num,h.answer_num,h.answer_lv,
                          |	year('${DATE6AGO}') leran_year, weekofyear('${DATE}') leran_week,h.pad_class is_pad,'${DATE}' time
                          |	from
                          |
                          |(
                          |    -----所有年级,所有学科,全部pad
                          |	select org_id,0 grade_id,0 subject_id,class_id,exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,0 grade_id,0 subject_id,class_id,0 exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,class_id,student_id
                          |	union all
                          |	-----所有年级,所有学科,分pad
                          |	select org_id,0 grade_id,0 subject_id,class_id,exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,pad_class,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,0 grade_id,0 subject_id,class_id,0 exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,pad_class,class_id,student_id
                          |	union all
                          |	-----各个年级,所有学科,全部pad
                          |	select org_id,grade_id,0 subject_id,class_id,exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,grade_id,0 subject_id,class_id,0 exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,class_id,student_id
                          |	union all
                          |	--各个年级,所有学科,分pad
                          |	select org_id,grade_id,0 subject_id,class_id,exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,pad_class,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,grade_id,0 subject_id,class_id,0 exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,pad_class,class_id,student_id
                          |	union all
                          |	--各个年级,各个学科,全部pad
                          |	select org_id,grade_id,subject_id,class_id,exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,grade_id,subject_id,class_id,0 exercise_source,student_id,9999 pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,class_id,student_id
                          |	union all
                          |	--各个年级,各个学科,分pad
                          |	select org_id,grade_id,subject_id,class_id,exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |    from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,pad_class,class_id,exercise_source,student_id
                          |	union all
                          |	--所有指标
                          |	select org_id,grade_id,subject_id,class_id,0 exercise_source,student_id,pad_class,
                          |	count(question_id) answer_num,
                          |	((sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end))/count(question_id)) answer_lv,
                          |	sum(case when ret_num=1 then 1
                          |	         when ret_num=5 then 0.5 else 0 end) right_num,
                          |	sum(case when ret_num=2 then 1
                          |			     when ret_num=5 then 0.5
                          |	         when ret_num=7 then 1 else 0 end) wrong_num,
                          |	sum(case when ret_num=6 then 1 else 0 end) unknown_num
                          |    from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,pad_class,class_id,student_id
                          |) h) h1 """.stripMargin
    sql(str_create_sws)

    val str_drop_swd=""" drop table test.student_weekly_details """.stripMargin
    sql(str_drop_swd)
    val str_create_swd=s""" create table test.student_weekly_details as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(   select distinct h.org_id,h.grade_id,h.class_id,h.student_id system_id,2 public_private,h.subject_id,
                          |	h.right_num answer_right_num,case when (h.right_num/h.all_num) is not null then (h.right_num/h.all_num) else 0 end answer_right_lv ,h.all_num answer_all_num,
                          |	h.zz_right_num auto_right_num,case when (h.zz_right_num/h.zz_num) is not null then  (h.zz_right_num/h.zz_num) else 0 end auto_right_lv,
                          |	h.zz_num auto_answer_num,
                          |	h.fz_right_num no_auto_right_num,case when (h.fz_right_num/h.fz_num) is not null then (h.fz_right_num/h.fz_num) else  0 end no_auto_right_lv,h.fz_num no_auto_answer_num,
                          |	year('${DATE6AGO}') leran_year, weekofyear('${DATE}') leran_week,h.pad_class is_pad,'${DATE}' time
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,class_id,student_id
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,pad_class,class_id,student_id
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,class_id,student_id
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,pad_class,class_id,student_id
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,class_id,student_id
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
                          |	         when exercise_source  in (1,6,7,8) and ret_num=5 then 0.5 else 0 end) fz_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,pad_class,class_id,student_id
                          |) h) h1 """.stripMargin
    sql(str_create_swd)

    val str_drop_swa="""  drop table test.student_weekly_autonomy """.stripMargin
    sql(str_drop_swa)
    val str_create_swa=s""" create table test.student_weekly_autonomy as
                          |select cast(concat('${DAY}',row_number() over()) as bigint) id,h1.* from
                          |(	select distinct h.org_id,h.grade_id,h.class_id,h.student_id system_id,2 public_private,h.subject_id,
                          |	h.wrong_right_num,case when (h.wrong_right_num/h.wrong_num) is not null then (h.wrong_right_num/h.wrong_num) else 0 end wrong_right_lv,h.wrong_num,
                          |	h.brush_right_num,case when (h.brush_right_num/h.brush_num) is not null then (h.brush_right_num/h.brush_num) else 0 end  brush_right_lv,h.brush_num,
                          |	h.groom_right_num,case when (h.groom_right_num/h.groom_num) is not null then (h.groom_right_num/h.groom_num) else 0 end  groom_right_lv,h.groom_num,
                          |	h.challenge_right_num,
                          |	case when (h.challenge_right_num/h.challenge_num) is not null then (h.challenge_right_num/h.challenge_num) else 0 end  challenge_right_lv,
                          |	h.challenge_num,
                          |	h.redo_right_num,case when (h.redo_right_num/h.redo_num) is not null then (h.redo_right_num/h.redo_num) else 0 end redo_right_lv,h.redo_num,
                          |	year('${DATE6AGO}') leran_year, weekofyear('${DATE}') leran_week,h.pad_class is_pad,'${DATE}' time
                          |	from
                          |(	select org_id,0 grade_id,0 subject_id,class_id,student_id,9999 pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,class_id,student_id
                          |	union all
                          |	-----所有年级，pad
                          |	select org_id,0 grade_id,0 subject_id,class_id,student_id,pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,pad_class,class_id,student_id
                          |	union all
                          |	--各个年级
                          |	select org_id,grade_id,0 subject_id,class_id,student_id,9999 pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,class_id,student_id
                          |	union all
                          |	--各个年级，pad
                          |	select org_id,grade_id,0 subject_id,class_id,student_id,pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,pad_class,class_id,student_id
                          |	union all
                          |	---各个年级学科
                          |	select org_id,grade_id,subject_id,class_id,student_id,9999 pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,class_id,student_id
                          |	union all
                          |	---各个年级学科，pad
                          |	select org_id,grade_id,subject_id,class_id,student_id,pad_class,
                          |	sum(case when exercise_source=3 then 1 else 0 end) wrong_num,
                          |	sum(case when exercise_source=3 and ret_num=1 then 1
                          |	         when exercise_source=3 and ret_num=5 then 0.5 else 0 end) wrong_right_num,
                          |
                          |	sum(case when exercise_source=5 then 1 else 0 end) brush_num,
                          |	sum(case when exercise_source=5 and ret_num=1 then 1
                          |	         when exercise_source=5 and ret_num=5 then 0.5 else 0 end) brush_right_num,
                          |
                          |	sum(case when exercise_source=9 then 1 else 0 end) groom_num,
                          |	sum(case when exercise_source=9 and ret_num=1 then 1
                          |	         when exercise_source=9 and ret_num=5 then 0.5 else 0 end) groom_right_num,
                          |
                          |	sum(case when exercise_source=4 then 1 else 0 end) challenge_num,
                          |	sum(case when exercise_source=4 and ret_num=1 then 1
                          |	         when exercise_source=4 and ret_num=5 then 0.5 else 0 end) challenge_right_num,
                          |
                          |	sum(case when exercise_source=10 then 1 else 0 end) redo_num,
                          |	sum(case when exercise_source=10 and ret_num=1 then 1
                          |	         when exercise_source=10 and ret_num=5 then 0.5 else 0 end) redo_right_num
                          |	from test.school_weekly_question_week
                          |	group by org_id,grade_id,subject_id,pad_class,class_id,student_id
                          |) h) h1 """.stripMargin
    sql(str_create_swa)
    sc_1.stop()
  }
}
