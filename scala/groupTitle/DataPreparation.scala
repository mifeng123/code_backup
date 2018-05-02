package groupTitle

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DataPreparation {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("DataPreparation")
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
    val data_1 = """drop table test.fabu""".stripMargin

    val data_2 = """create table test.fabu as
                 |select
                 |	lkeq.question_id,
                 |	count(distinct pub.id) pub_cnt
                 |from neworiental_v3.entity_respackage_publish pub
                 |inner join neworiental_user.entity_user us
                 |on us.system_id = pub.creator_id
                 |inner join (select org_id,case when private=0 then 2 else 4 end org_type,sch_name,enable
                 |	from neworiental_logdata.entity_school) sch
                 |on us.org_id = sch.org_id and us.org_type = sch.org_type
                 |inner join neworiental_v3.link_respackage_publish_resource lkrpr
                 |on lkrpr.publish_id = pub.id
                 |inner join neworiental_v3.link_exercise_question lkeq
                 |on lkeq.question_id = lkrpr.resource_id
                 |where sch.enable = 1
                 |	and lkrpr.resource_type = 2
                 |group by lkeq.question_id""".stripMargin

    val data_3 = """drop table test.dati""".stripMargin


    val data_4 = """create table test.dati as
                 |select
                 |	se.question_id,
                 |	count(1) cnt
                 |from student_answer.student_answer_data se
                 |where se.submit_time is not null
                 |	and se.ret_num > 0
                 |group by se.question_id""".stripMargin

    val data_5 = """drop table test.que_fenzhi""".stripMargin

    val data_6 = """create table test.que_fenzhi as
                 |select
                 |	que.id,
                 |	case when bhl.header_status is null then 0 else 1 end as header_status,
                 |	qq.score,
                 |	case when (fb.pub_cnt*0.01) is not null then (fb.pub_cnt*0.01) else 0 end fff1,
                 |	case when (dt.cnt*0.001) is not null then (dt.cnt*0.001) else 0 end fff2,
                 |	case when (qq.score*10) is not null then (qq.score*10) else 0 end fff3,
                 |	case when que.upload_src = 105 then 1 else 0 end as mt,
                 |	case when que.upload_src = 1000 or que.upload_src = 1001 then 1 else 0 end as tj,
                 |	que.question_type_id,
                 |	que.difficulty,
                 |	lkqt.topic_id
                 |from neworiental_v3.entity_question que
                 |inner join neworiental_v3.link_question_topic lkqt
                 |on lkqt.question_id = que.id
                 |left join neworiental_v3.entity_question_quality qq
                 |on qq.question_id = que.id
                 |left join test.fabu fb
                 |on fb.question_id = que.id
                 |left join test.dati dt
                 |on dt.question_id = que.id
                 |left join test.bushu_hque_lack bhl
                 |on bhl.question_id = que.id
                 |where que.new_format = 1
                 |	and que.state = 'ENABLED'
                 |	and que.parent_question_id = 0""".stripMargin

    /*val data_7 = """CREATE TABLE if not exists test.sf_question_score (
                 |	`id` String,
                 |	`question_id` bigint,
                 |	`score` double,
                 |	`question_type_id` int,
                 |	`difficulty` int,
                 |	`topic_id` bigint)""".stripMargin*/
    val data_7 ="""  drop table test.sf_question_score """.stripMargin

    val data_8 = """create  table test.sf_question_score as
                 |select
                 |	concat(zt.topic_id, '_', zt.question_type_id, '_', zt.id) id ,
                 |	zt.id question_id,
                 |	zt.score,
                 |	zt.question_type_id,
                 |	zt.difficulty,
                 |	zt.topic_id
                 |from (
                 |	select
                 |		sf.id,
                 |		case when sf.header_status = 0 or sf.score is null or sf.fen is null then 0 else sf.fen end score,
                 |		sf.question_type_id,
                 |		sf.difficulty,
                 |		sf.topic_id
                 |	from (
                 |		select
                 |			hz.id,
                 |			hz.header_status,
                 |			hz.score,
                 |			(hz.fff1 + hz.fff2 + hz.fff3 + hz.header_status*5 + hz.mt*3 + hz.tj*3) fen,
                 |			hz.question_type_id,
                 |			hz.difficulty,
                 |			hz.topic_id
                 |		from test.que_fenzhi hz
                 |	) sf ) zt""".stripMargin

    /*val data_9 = """CREATE TABLE if not exists test.sf_link_topic_question (
                 |	`id` String,
                 |	`question_id` bigint,
                 |	`question_type_id` bigint)""".stripMargin*/
    val data_9=""" drop table  test.sf_link_topic_question """.stripMargin

    val data_10 = """create  table test.sf_link_topic_question as
                  |select
                  |	concat(qt.topic_id, '_', que.question_type_id, '_', row_number() over(partition by qt.topic_id, que.question_type_id)) id,
                  |	qt.question_id,
                  |	que.question_type_id
                  |from neworiental_v3.link_question_topic qt
                  |inner join neworiental_v3.entity_question que
                  |on qt.question_id = que.id
                  |where que.new_format = 1
                  |	and que.state = 'ENABLED'
                  |	and que.parent_question_id = 0""".stripMargin

    /*val data_11 = """CREATE TABLE if not exists test.sf_link_topic_cnt (
                  |	`id` String,
                  |	`question_id` bigint,
                  |	`question_type_id` bigint,
                  |	`cnt` bigint)""".stripMargin*/
    val data_11 ="""  drop table test.sf_link_topic_cnt  """.stripMargin

    val data_12 = """create  table test.sf_link_topic_cnt as
                  |select
                  |	concat(lkqt.topic_id, '_', que.question_type_id) id,
                  |	lkqt.topic_id topic_id,
                  |	que.question_type_id,
                  |	count(lkqt.question_id) cnt
                  |from neworiental_v3.link_question_topic lkqt
                  |inner join neworiental_v3.entity_question que
                  |on lkqt.question_id = que.id
                  |where que.new_format = 1
                  |	and que.state = 'ENABLED'
                  |	and que.parent_question_id = 0
                  |group by lkqt.topic_id,
                  |	que.question_type_id""".stripMargin

    /*val data_13 = """CREATE TABLE if not exists test.sf_student_exercise_new (
                  |	`id` String,
                  |	`question_id` bigint,
                  |	`student_id` bigint,
                  |	`wrong` int,
                  |	`topic_id` bigint,
                  |	`question_type_id` bigint)""".stripMargin*/
    val data_13="""  drop table test.sf_student_exercise_new """.stripMargin

    val data_14 = """create table test.sf_student_exercise_new as
                  |select
                  |	concat(sen.topic_id, '_', sen.student_id, '_', sen.wrong, '_',sen.question_type_id,'_', row_number() over(partition by sen.topic_id, sen.student_id, sen.wrong, sen.question_type_id)) id,
                  |	sen.question_id,
                  |	sen.student_id,
                  |	sen.wrong,
                  |	sen.topic_id,
                  |	sen.question_type_id
                  |from (
                  |	select
                  |		que.id question_id,
                  |		sen.student_id,
                  |		case when sen.ret_num = 1 then 1 else 2 end as wrong,
                  |		lkqt.topic_id,
                  |		que.question_type_id
                  |	from neworiental_v3.entity_student_exercise_new sen
                  |	inner join neworiental_v3.entity_question que
                  |	on que.id = sen.question_id
                  |	inner join neworiental_v3.link_question_topic lkqt
                  |	on lkqt.question_id = que.id
                  |	where que.new_format = 1
                  |		and que.state = 'ENABLED'
                  |		and que.parent_question_id = 0
                  |		and sen.ret_num > 0
                  |		and sen.ret_num != 6
                  |		and sen.submit_time is not null
                  |		and sen.submit_time != 'null'
                  |) sen""".stripMargin

    /*val data_15 = """CREATE TABLE if not exists test.sf_student_exercise_new_cnt (
                  |	`id` String,
                  |	`student_id` bigint,
                  |	`wrong` int,
                  |	`topic_id` bigint,
                  |	`question_type_id` bigint,
                  |	`cnt` bigint)""".stripMargin*/
    val data_15="""  drop table test.sf_student_exercise_new_cnt  """.stripMargin

    val data_16 = """create table test.sf_student_exercise_new_cnt as
                  |select
                  |	concat(sen.topic_id, '_', sen.student_id, '_', sen.wrong,'_',sen.question_type_id) id,
                  |	sen.student_id,
                  |	sen.wrong,
                  |	sen.topic_id,
                  |	sen.question_type_id,
                  |	count(sen.question_id) cnt
                  |from (
                  |	select
                  |		que.id question_id,
                  |		sen.student_id,
                  |		case when sen.ret_num = 1 then 1 else 2 end as wrong,
                  |		lkqt.topic_id,
                  |		que.question_type_id
                  |	from neworiental_v3.entity_student_exercise_new sen
                  |	inner join neworiental_v3.entity_question que
                  |	on que.id = sen.parent_question_id
                  |	inner join neworiental_v3.link_question_topic lkqt
                  |	on lkqt.question_id = que.id
                  |	where que.new_format = 1
                  |		and que.state = 'ENABLED'
                  |		and que.parent_question_id = 0
                  |		and sen.ret_num > 0
                  |		and sen.ret_num != 6
                  |		and sen.submit_time is not null
                  |		and sen.submit_time != 'null'
                  |) sen
                  |group by sen.student_id,
                  |	sen.wrong,
                  |	sen.topic_id,
                  |	sen.question_type_id""".stripMargin

    val data_17 = """drop table test.yinyong""".stripMargin

    val data_18 = """create table test.yinyong as
                  |select
                  |	lkeq.question_id,
                  |	count(distinct rep.id) pub_cnt
                  |from neworiental_v3.entity_respackage rep
                  |inner join neworiental_user.entity_user us
                  |on us.system_id = rep.creator_id
                  |inner join (select org_id,case when private=0 then 2 else 4 end org_type,sch_name,enable
                  |	from neworiental_logdata.entity_school) sch
                  |on us.org_id = sch.org_id and us.org_type = sch.org_type
                  |inner join neworiental_v3.link_respackage_resource lkrr
                  |on lkrr.package_id = rep.id
                  |inner join neworiental_v3.link_exercise_question lkeq
                  |on lkeq.exercise_id = lkrr.resource_id
                  |where sch.enable = 1
                  |	and lkrr.resource_type = 2
                  |group by lkeq.question_id""".stripMargin

    val data_19 = """drop table test.question_time""".stripMargin

    val data_20 = """create table test.question_time as
                  |select
                  |	asr.question_id,
                  |	sum(case when asr.answer_dur > 0 then asr.answer_dur else 0 end) total_time,
                  |	sum(case when asr.answer_dur > 0 then asr.answer_dur else 0 end)/count(1) avg_time
                  |from neworiental_middle.base_action_student_question_answer_tmp asr
                  |inner join neworiental_user.entity_user us
                  |on asr.uid = us.id
                  |inner join (select org_id,case when private=0 then 2 else 4 end org_type,sch_name,enable
                  |	from neworiental_logdata.entity_school) sch
                  |on us.org_id = sch.org_id and us.org_type = sch.org_type
                  |where sch.enable = 1
                  |	and asr.answer_dur/1000<=3600
                  |	and asr.answer_dur > 0
                  |group by asr.question_id""".stripMargin

    /*val data_21 = """CREATE TABLE if not exists test.sf_question_out (
                  |	`id` String,
                  |	`total_num` bigint,
                  |	`ratio` double,
                  |	`pub_cnt` bigint,
                  |	`avg_time` double)""".stripMargin*/

   val data_21=""" drop table  test.sf_question_out """.stripMargin
    val data_22 = """create  table test.sf_question_out as
                  |select
                  |	cast(que.id as String) id,
                  |	case when sum(sen.total_num) is null then 0 else sum(sen.total_num) end total_num,
                  |	case when (sum(sen.ratio)/count(sen.ratio))  is null then 0 else (sum(sen.ratio)/count(sen.ratio)) end ratio,
                  |	case when yy.pub_cnt is null then 0 else yy.pub_cnt end pub_cnt,
                  |	case when qt.avg_time is null then 0 else qt.avg_time end avg_time
                  |from neworiental_v3.entity_question que
                  |left join (
                  |	select * from neworiental_v3.entity_student_exercise_new sen
                  |	where sen.ret_num > 0
                  |		and sen.ret_num != 6
                  |		and sen.submit_time is not null
                  |		and sen.submit_time != 'null' ) sen
                  |on que.id = sen.question_id
                  |left join test.yinyong yy
                  |on yy.question_id = que.id
                  |left join test.question_time qt
                  |on qt.question_id = que.id
                  |where que.new_format = 1
                  |	and que.state = 'ENABLED'
                  |	and que.parent_question_id = 0
                  |group by que.id,
                  |	yy.pub_cnt,
                  |	qt.avg_time""".stripMargin
    sql(data_1)
    sql(data_2)
    sql(data_3)
    sql(data_4)
    sql(data_5)
    sql(data_6)
    sql(data_7)
    sql(data_8)
    sql(data_9)
    sql(data_10)
    sql(data_11)
    sql(data_12)
    sql(data_13)
    sql(data_14)
    sql(data_15)
    sql(data_16)
    sql(data_17)
    sql(data_18)
    sql(data_19)
    sql(data_20)
    sql(data_21)
    sql(data_22)

    sc_1.stop()
  }
}
