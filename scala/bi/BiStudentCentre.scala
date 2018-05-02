package bi

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
object BiStudentCentre {
  def main(args: Array[String]): Unit = {
    val cnx = new SparkConf().setAppName("spark_sql_wej")
    val sc = new SparkContext(cnx)
    val hiveCtx = new HiveContext(sc)

    val hive_set = "set hive.exec.dynamic.partition=true"
    val hive_set2 = "set hive.exec.dynamic.partition.mode=nonstrict"
    val hive_set3 = "set hive.exec.max.dynamic.partitions.pernode=10000"
    val hive_set4 = "set mapred.reduce.tasks = 15"



    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-3)
    val yesterday5 = dateFormat.format(cal.getTime())
    println(yesterday5)
    cal.add(Calendar.DATE,2)
    val yesterday = dateFormat.format(cal.getTime())
    println(yesterday)

    var now:Date = new Date()
    val today = dateFormat.format( now )
    println(today)

    var period:String=""
    var cal2:Calendar =Calendar.getInstance();
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period=dateFormat.format(cal.getTime())
    println(period)
    var sql1:String = null
    var sql2:String = null
  val DATENOW = args(0).toString
  val DATE = args(1).toString
  val sqlc1 = """create table if not exists neworiental_report.bi_studay_centre_top (
               |	student_id bigint,
               |	stu_name String,
               |	mdl_id bigint,
               |	mdl_name String,
               |	unt_id bigint,
               |	unt_name String,
               |	top_id bigint,
               |	top_name String,
               |	total int,
               |	r1 int,
               |	r2 int,
               |	rote double,
               |	r5 int,
               |	r6 int,
               |	master double,
               | subject_name String,
               | time  date
               |)""".stripMargin

 val sqlc2 = """create table if not exists neworiental_report.bi_studay_centre_chp (
               |	student_id bigint,
               |	name String,
               |	l1id bigint,
               |	l1name String,
               |	l2id bigint,
               |	l2name String,
               |	l3id bigint,
               |	l3name String,
               |	total int,
               |	r1 int,
               |	r2 int,
               |	rote double,
               |	r5 int,
               |	r6 int,
               | subject_name String,
               | version_name String,
               | time date
               |)""".stripMargin

 if (today == period) {
   println("==")
   sql1 = s""" insert overwrite table neworiental_report.bi_studay_centre_top
             |select
             |	sad.student_id,
             |	us.name,
             |	mdl.id,
             |	mdl.name,
             |	unt.id,
             |	unt.name,
             |	top.id,
             |	top.name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,
             |	rtc.master,ese.fullname subject_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_topic lkqt
             |on lkqt.question_id = sad.question_id
             |inner join neworiental_v3.entity_topic top
             |on top.id = lkqt.topic_id
             |inner join neworiental_v3.entity_subject ese on ese.id=top.subject_id
             |inner join neworiental_v3.entity_unit unt
             |on unt.id = top.unit_id
             |inner join neworiental_v3.entity_module mdl
             |on mdl.id = unt.module_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join logdata.real_time_computation_xdzwcd_1 rtc
             |on rtc.system_id = sad.student_id and rtc.topic_id = top.id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where  sad.ret_num > 0
             |	and sad.submit_time >= '${yesterday5}'
             |	and sad.submit_time < '${today}'
             |group by to_date(sad.submit_time),ese.fullname,sad.student_id,
             |	us.name,
             |	mdl.id,
             |	mdl.name,
             |	unt.id,
             |	unt.name,
             |	top.id,
             |	top.name,
             |	rtc.master """.stripMargin

   sql2 = s""" insert overwrite table neworiental_report.bi_studay_centre_chp
             | select
             |	sad.student_id,
             |	us.name,
             |	l1.id l1id,
             |	l1.name l1name,
             |	l2.id l2id,
             |	l2.name l2name,
             |	chp.id l3id,
             |	chp.name l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc
             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join (
             |		select
             |			id,
             |			name,
             |			parent_id
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 2) l2
             |	on l2.id = chp.parent_id
             |inner join (
             |		select
             |			id,
             |			name
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 1
             |	) l1
             |	on l1.id = l2.parent_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and sad.submit_time >= '${yesterday5}'
             |	and sad.submit_time < '${today}'
             |	and chp.level = 3
             |group by to_date(sad.submit_time),ese.fullname ,etd.version_name, sad.student_id,
             |	us.name,
             |	l1.id,
             |	l1.name,
             | l2.id,
             |	l2.name,
             |	chp.id,
             |	chp.name
             |union all
             |select
             |	sad.student_id,
             |	us.name,
             |	l1.id l1id,
             |	l1.name l1name,
             |	chp.id l2id,
             |	chp.name l2name,
             |	0 l3id,
             |	'N' l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc
             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join (
             |		select
             |			id,
             |			name
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 1
             |	) l1
             |	on l1.id = chp.parent_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and chp.level = 2
             |	and sad.submit_time >= '${yesterday5}'
             |	and sad.submit_time < '${today}'
             |group by to_date(sad.submit_time),ese.fullname ,etd.version_name,sad.student_id,
             |	us.name,
             |	l1.id,
             |	l1.name,
             |	chp.id,
             |	chp.name
             |union all
             |select
             |	sad.student_id,
             |	us.name,
             |	chp.id l1id,
             |	chp.name l1name,
             |	0 l2id,
             |	'N' l2name,
             |	0 l3id,
             |	'N' l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc
             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and chp.level = 1
             |	and sad.submit_time >= '${yesterday5}'
             |	and sad.submit_time < '${today}'
             |group by to_date(sad.submit_time),ese.fullname,etd.version_name,sad.student_id,
             |	us.name,
             |	chp.id,
             |	chp.name """.stripMargin


 } else {
   sql1 = s""" insert overwrite table neworiental_report.bi_studay_centre_top
             |select
             |	sad.student_id,
             |	us.name,
             |	mdl.id,
             |	mdl.name,
             |	unt.id,
             |	unt.name,
             |	top.id,
             |	top.name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,
             |	rtc.master,ese.fullname subject_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_topic lkqt
             |on lkqt.question_id = sad.question_id
             |inner join neworiental_v3.entity_topic top
             |on top.id = lkqt.topic_id
             |inner join neworiental_v3.entity_subject ese on ese.id=top.subject_id
             |inner join neworiental_v3.entity_unit unt
             |on unt.id = top.unit_id
             |inner join neworiental_v3.entity_module mdl
             |on mdl.id = unt.module_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join logdata.real_time_computation_xdzwcd_1 rtc
             |on rtc.system_id = sad.student_id and rtc.topic_id = top.id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and sad.submit_time >= '${DATE}'
             |	and sad.submit_time < '${DATENOW}'
             |group by to_date(sad.submit_time),ese.fullname,sad.student_id,
             |	us.name,
             |	mdl.id,
             |	mdl.name,
             |	unt.id,
             |	unt.name,
             |	top.id,
             |	top.name,
             |	rtc.master """.stripMargin

   sql2 = s"""insert overwrite table neworiental_report.bi_studay_centre_chp
             |select
             |	sad.student_id,
             |	us.name,
             |	l1.id l1id,
             |	l1.name l1name,
             |	l2.id l2id,
             |	l2.name l2name,
             |	chp.id l3id,
             |	chp.name l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc

             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join (
             |		select
             |			id,
             |			name,
             |			parent_id
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 2) l2
             |	on l2.id = chp.parent_id
             |inner join (
             |		select
             |			id,
             |			name
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 1
             |	) l1
             |	on l1.id = l2.parent_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and sad.submit_time >= '${DATE}'
             |	and sad.submit_time < '${DATENOW}'
             |	and chp.level = 3
             |group by to_date(sad.submit_time),ese.fullname,etd.version_name,sad.student_id,
             |	us.name,
             |	l1.id,
             |	l1.name,
             |	l2.id,
             |	l2.name,
             |	chp.id,
             |	chp.name
             |
            |union all
             |
            |select
             |	sad.student_id,
             |	us.name,
             |	l1.id l1id,
             |	l1.name l1name,
             |	chp.id l2id,
             |	chp.name l2name,
             |	0 l3id,
             |	'N' l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc
             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join (
             |		select
             |			id,
             |			name
             |		from neworiental_v3.entity_teaching_chapter
             |		where level = 1
             |	) l1
             |	on l1.id = chp.parent_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and chp.level = 2
             |	and sad.submit_time >= '${DATE}'
             |	and sad.submit_time < '${DATENOW}'
             |group by to_date(sad.submit_time),ese.fullname,etd.version_name,sad.student_id,
             |	us.name,
             |	l1.id,
             |	l1.name,
             |	chp.id,
             |	chp.name
             |
            |union all
             |
            |select
             |	sad.student_id,
             |	us.name,
             |	chp.id l1id,
             |	chp.name l1name,
             |	0 l2id,
             |	'N' l2name,
             |	0 l3id,
             |	'N' l3name,
             |	count(distinct sad.question_id) total,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end) r1,
             |	sum(case when sad.ret_num = 2 then 1 else 0 end) r2,
             |	sum(case when sad.ret_num = 1 then 1 else 0 end)/count(distinct sad.student_id, sad.question_id, sad.submit_time) rote,
             |	sum(case when sad.ret_num = 5 then 1 else 0 end) r5,
             |	sum(case when sad.ret_num = 6 then 1 else 0 end) r6,ese.fullname subject_name,etd.version_name,to_date(sad.submit_time) time
             |from neworiental_v3.entity_student_exercise_new sad
             |inner join neworiental_v3.link_question_chapter lkqc
             |on lkqc.question_id = sad.question_id
             |inner join neworiental_v3.entity_teaching_chapter chp
             |on chp.id = lkqc.chapter_id
             |inner join neworiental_v3.entity_subject ese on ese.id=chp.subject_id
             |inner join neworiental_v3.entity_teaching_directory etd on etd.id=chp.directory_id
             |inner join neworiental_user.entity_user us
             |on us.system_id = sad.student_id
             |inner join test.bi_student_centre bsc on bsc.student_id= sad.student_id
             |where sad.ret_num > 0
             |	and chp.level = 1
             |	and sad.submit_time >= '${DATE}'
             |	and sad.submit_time < '${DATENOW}'
             |group by to_date(sad.submit_time),ese.fullname,etd.version_name,sad.student_id,
             |	us.name,
             |	chp.id,
             |	chp.name """.stripMargin
 }

 hiveCtx.sql(hive_set)
 hiveCtx.sql(hive_set2)
 hiveCtx.sql(hive_set3)
 hiveCtx.sql(hive_set4)
 hiveCtx.sql(sqlc1)
 hiveCtx.sql(sqlc2)
 hiveCtx.sql(sql1)
 hiveCtx.sql(sql2)

 sc.stop()

}
}

