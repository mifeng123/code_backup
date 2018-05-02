package bi

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object TopicQuestioUsage {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("bi_head_question")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.speculation", "true")
      .set("spark.task.maxFailures", "8")
      .set("spark.akka.timeout", "300")
      .set("spark.network.timeout", "300")
      .set("spark.yarn.max.executor.failures", "100")
    val sc_1 = new SparkContext(sparkConf)
    val hc_1 = new HiveContext(sc_1)
    import hc_1.sql
    /*val DATENOW = args(0).toString
    val DAY = args(1).toString
    val DATE = args(2).toString
    val DAY6AGO = args(4).toString*/
   val str_drop_tqu=""" drop table test.topic_question_usage """.stripMargin
    sql(str_drop_tqu)
    val str_create_tqu="""  create table test.topic_question_usage as
                         |select tmp_1.topic_id,
                         |if(tmp_2.head_num  is null,0, tmp_2.head_num )  head_num ,
                         |if(tmp_2.abandon_num is null,0,tmp_2.abandon_num)  abandon_num,
                         |if(tmp_1.dis_question_num is null,0,tmp_1.dis_question_num)  dis_question_num,
                         |if(tmp_2.pend_num is null,0,tmp_2.pend_num)  pend_num,
                         |if(tmp_1.ena_question_num is null,0,tmp_1.ena_question_num)  ena_question_num
                         |from
                         |(
                         |select  et.id topic_id,
                         |sum(case when que.state = 'DISABLED' then 1 else 0 end) dis_question_num,
                         |sum(case when que.state = 'ENABLED' then 1 else 0 end) ena_question_num,
                         |count(distinct lkqt.question_id)  total_question_num
                         | from neworiental_v3.link_question_topic lkqt
                         | inner join neworiental_v3.entity_question que on que.id = lkqt.question_id
                         | inner join neworiental_v3.entity_topic et on et.id = lkqt.topic_id
                         | where que.new_format = 1 	and (que.state = 'ENABLED' or que.state = 'DISABLED')
                         |and que.parent_question_id = 0 	and et.status = 1
                         |group by  et.id) tmp_1
                         |--知识点下的 待处理 头部  弃选
                         |left join
                         |(select et.id topic_id,
                         |sum(case when yqit.header_status=0 then 1 else 0 end ) pend_num,
                         |sum(case when yqit.header_status=1 then 1 else 0 end ) head_num,
                         |sum(case when yqit.header_status=2 then 1 else 0 end ) abandon_num
                         | from neworiental_v3.yunying_question_info_topic yqit
                         |join neworiental_v3.entity_topic et on et.id=yqit.topic_id
                         |where yqit.state='ENABLED'
                         |GROUP BY et.id) tmp_2 on tmp_1.topic_id=tmp_2.topic_id """.stripMargin
    sql(str_create_tqu)
    val  str_drop_thq=""" drop table test.topic_head_question """.stripMargin
    sql(str_drop_thq)
    val str_create_thq="""  create  table test.topic_head_question as
                         |select s.id stage_id,s.name stage_name,j.id subject_id,j.name subject_name,m.id module_id,m.name module_name,u.id unit_id,
                         |u.name unit_name,t.id topic_id,t.name topic_name,
                         |tqu.head_num,tqu.abandon_num,tqu.dis_question_num,tqu.pend_num,tqu.ena_question_num
                         |from neworiental_v3.entity_topic t
                         |join test.topic_question_usage tqu on tqu.topic_id=t.id
                         |join neworiental_v3.entity_stage s on t.stage_id=s.id
                         |join neworiental_v3.entity_subject j on t.subject_id=j.id
                         |join neworiental_v3.entity_unit u on t.unit_id=u.id
                         |join neworiental_v3.entity_module m on u.module_id=m.id
                         |where t.status=1  """.stripMargin
    sql(str_create_thq)
    sc_1.stop()
  }
}
