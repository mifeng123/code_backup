package bi

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object ChapterQuestionUsage {
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
    val str_drop_cqu=""" drop table test.chapter_question_usage """.stripMargin
    sql(str_drop_cqu)
    val str_create_cqu=""" create table test.chapter_question_usage as
                         |select tmp_1.chapter_id,
                         |if(tmp_2.head_num  is null,0, tmp_2.head_num )  head_num ,
                         |if(tmp_2.abandon_num is null,0,tmp_2.abandon_num)  abandon_num,
                         |if(tmp_1.dis_question_num is null,0,tmp_1.dis_question_num)  dis_question_num,
                         |if(tmp_2.pend_num is null,0,tmp_2.pend_num)  pend_num,
                         |if(tmp_1.ena_question_num is null,0,tmp_1.ena_question_num)  ena_question_num
                         |from
                         |(select chp.subject_id, chp.id chapter_id,
                         |sum(case when que.state = 'DISABLED' then 1 else 0 end) dis_question_num,
                         |sum(case when que.state = 'ENABLED' then 1 else 0 end) ena_question_num
                         |from neworiental_v3.link_question_chapter lkqc
                         |inner join neworiental_v3.entity_teaching_chapter chp on chp.id = lkqc.chapter_id
                         |inner join neworiental_v3.entity_question que on que.id = lkqc.question_id
                         |where que.new_format = 1 and (que.state = 'ENABLED' or que.state = 'DISABLED') and que.parent_question_id = 0
                         |group by chp.subject_id, chp.id
                         |) tmp_1
                         |--章节下的头部题目
                         |left join
                         |(select yqic.chapter_id,
                         |sum(case when yqic.header_status=0 then 1 else 0 end ) pend_num,
                         |sum(case when yqic.header_status=1 then 1 else 0 end ) head_num,
                         |sum(case when yqic.header_status=2 then 1 else 0 end ) abandon_num
                         | from neworiental_v3.yunying_question_info_chapter yqic
                         |join neworiental_v3.entity_teaching_chapter etc on etc.id=yqic.chapter_id
                         |where state='ENABLED'
                         |GROUP BY yqic.chapter_id) tmp_2 on tmp_1.chapter_id=tmp_2.chapter_id  """.stripMargin
    sql(str_create_cqu)
    val  str_drop_chq=""" drop table test.chapter_head_question """.stripMargin
    sql(str_drop_chq)
    val str_create_chq="""  create table test.chapter_head_question as
                         |select s.id stage_id,s.name stage_name,j.id subject_id,j.name subject_name,d.version_id,d.version_name,d.grade_id,d.grade_name,
                         |    c.id1 chapter_1_id,c.name1 chapter_1_name,if(c.id2 is null,0,c.id2) chapter_2_id,if(c.name2 is null,'空',c.name2) chapter_2_name,
                         |	if(c.id3 is null,0,c.id3) chapter_3_id,if(c.name3 is null,'空',c.name3) chapter_3_name ,
                         |cqu.head_num,cqu.abandon_num,cqu.dis_question_num,cqu.pend_num,cqu.ena_question_num
                         |from(
                         |	select c1.id id1,concat(c1.prefix_name,c1.name) name1,c2.id id2,concat(c2.prefix_name,c2.name) name2,c3.id id3,concat(c3.prefix_name,c3.name) name3,
                         |	if(c3.id is null,if(c2.id is null,c1.id,c2.id),c3.id) id,
                         |	c1.subject_id,c1.stage_id,c1.directory_id
                         |	from neworiental_v3.entity_teaching_chapter c1
                         |	left join neworiental_v3.entity_teaching_chapter c2
                         |	on c2.parent_id=c1.id and c2.level=2
                         |	left join neworiental_v3.entity_teaching_chapter c3
                         |	on c3.parent_id=c2.id and c2.level=2 and c3.level=3
                         |	where c1.level=1)c
                         |join test.chapter_question_usage cqu on cqu.chapter_id=c.id
                         |join neworiental_v3.entity_stage s on c.stage_id=s.id
                         |join neworiental_v3.entity_subject j on c.subject_id=j.id
                         |join neworiental_v3.entity_teaching_directory d on c.directory_id=d.id """.stripMargin
    sql(str_create_chq)
    sc_1.stop()
  }
}
