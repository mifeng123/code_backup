package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object UsageClass {
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
