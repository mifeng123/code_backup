package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object UsageSubject {
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
    sc_1.stop()
  }
}
