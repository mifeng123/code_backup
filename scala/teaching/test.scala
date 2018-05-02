package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object test {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("HiveAnswerSpark")
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
    val str=""" create table test.later_view as
              |select
              |	sen.parent_question_id id,
              |	sum(sen.total_num) total_num,
              |	sum(sen.ratio)/count(sen.ratio) ratio,
              |	case when yy.pub_cnt is null then 0 else yy.pub_cnt end  pub_cnt,
              |	case when qt.avg_time is null then 0 else qt.avg_time end question_type_id,
              |	case when analysis is null then '无' else analysis end analysis
              |from neworiental_v3.entity_student_exercise_new sen
              |inner join neworiental_v3.entity_question que
              |on que.id = sen.question_id
              |left join test.yinyong yy
              |on yy.question_id = sen.parent_question_id
              |left join test.question_time qt
              |on qt.question_id = sen.question_id
              |left join (
              |	select
              |		id,
              |		analysis
              |	from canal.canal_entity_question ceq
              |	lateral view json_tuple(html_data, "content") d as content
              |	lateral view json_tuple(content, "analysis") d as analysis ) ceq
              |on ceq.id = sen.question_id
              |where que.new_format = 1
              |	and que.state = 'ENABLED'
              |	and que.parent_question_id = 0
              |	and sen.ret_num > 0
              |	and sen.ret_num != 6
              |	and sen.submit_time is not null
              |	and sen.submit_time != 'null'
              |group by sen.parent_question_id,
              |	yy.pub_cnt,
              |	qt.avg_time,
              |	ceq.analysis """.stripMargin
    sql(str)
    sc_1.stop()
  }
}
