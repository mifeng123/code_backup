package SparkF

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ztf on 2017/8/7.
  */
object Spark_Streaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark Streamig")
    val sc = new SparkContext(conf)
    val hc= new HiveContext(sc)
    sc.stop()
    //Spark 用hive的udf函数 是不要add 这一步的  只需要在workflow  files配置中加进去就ok了
    /*val sql_str="""add jar hdfs//nameservice1:8020/user/hue/oozie/workspaces/hue-oozie-1498471962.36/HiveJsonAnswerjar.jar""".stripMargin
    hc.sql(sql_str)*/
  /*  val sql_str2= """create temporary function jsontest  as 'hiveJson.HiveJsonAnswer'""".stripMargin
    hc.sql(sql_str2)
    val sql_str3="""drop table if exists test.answer_text""".stripMargin
    hc.sql(sql_str3)
    val sql_str4="""create table  test.answer_text(qid bigint,sum array<String>) row format delimited FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' stored as textfile""".stripMargin
    hc.sql(sql_str4)
    val sql_str5="""insert overwrite table test.answer_text select a.qid,split(jsontest(cast(b.id as int),a.con,c.struct_id,cast(b.subject_id as int),c.type_id), ",") from question.question_json a JOIN neworiental_v3.entity_question b ON a.qid=b.id JOIN neworiental_v3.entity_question_type c ON b.question_type_id=c.type_id where b.state="ENABLED" AND b.new_format=1 AND b.parent_question_id=0""".stripMargin
    hc.sql(sql_str5)*/
/*    val  day_1 = args(0)
    val str = """create table test.spark_answer as
               |select
               |jg.question_id,qqh.difficulty,h.name,jg.num_2,jg.num_3,jg.num_4,h.num_type,
               |row_number() over(partition by h.num_type,h.name,jg.num_2,jg.num_3 order by jg.num_4 desc) row
               |from
               |(select h1.question_id question_id,
               | case when h1.num_2 is not null then h1.num_2 else 0 end num_2,
               | case when h1.num_3 is not null then h1.num_3 else 0 end num_3,
               | case when h1.num_4 is not null then h1.num_4 else 0 end num_4
               |from test.jiegou h1) jg
               |left join
               |(
               |select question_id same_id ,difficulty from neworiental_report.entity_question_quality_hive
               |union all
               |select qs.question_id same_id ,eq.difficulty difficulty  from neworiental_report.entity_question_same qs
               |left join  (select qs.question_id same_id ,eq.difficulty,eq.question_id,qs.same_num same_num from neworiental_report.entity_question_same qs
               |join neworiental_report.entity_question_quality_hive eq on eq.question_id=qs.question_id  ) eq
               |on eq.same_num=qs.same_num
               |) qqh on qqh.same_id=jg.question_id
               |join (select eq.id qid,et.name  name,1 num_type
               |from neworiental_v3.entity_question eq
               |join neworiental_v3.link_question_topic lqt on lqt.question_id=eq.id
               |join neworiental_v3.entity_topic et on et.id=lqt.topic_id
               |where eq.subject_id =4 and eq.state="ENABLED" AND eq.new_format=1 AND eq.parent_question_id=0
               |and et.status=1   and lqt.topic_id is not null
               |union all
               |select eq.id qid,etc.name name,2 num_type
               |from neworiental_v3.entity_question eq
               |join neworiental_v3.link_question_chapter lqc on lqc.question_id=eq.id
               |join neworiental_v3.entity_teaching_chapter etc on etc.id=lqc.chapter_id
               |where eq.subject_id =4 and eq.state="ENABLED" AND eq.new_format=1 AND eq.parent_question_id=0
               |and lqc.chapter_id is not null) h on h.qid=jg.question_id
               |group by h.num_type,h.name,qqh.difficulty,jg.question_id,jg.num_2,jg.num_3,jg.num_4""".stripMargin
    hc.sql(str)*/

  }
}
