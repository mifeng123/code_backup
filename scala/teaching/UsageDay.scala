package teaching

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object UsageDay {
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
    sc_1.stop()
  }
}
