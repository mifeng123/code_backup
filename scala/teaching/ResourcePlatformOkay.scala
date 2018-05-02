package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/10/29.
  */
object ResourcePlatformOkay {
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
    val DATE = args(0).toString
    val DATENOW = args(1).toString
    val str_all=s""" insert into table test.resource_platform_tmp
                  |select count(c.id) num,
                  |    count(case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then c.id end) num_week,
                  |	count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and share_time<'${DATENOW}' then c.id end) month_week,
                  |    1 num_type,subject_id,stage_id
                  |from neworiental_v3.entity_res_courseware c
                  |where platform_check_status=0 and subject_id is not null and stage_id is not null and c.status=1
                  |group by subject_id,stage_id
                  |union all
                  |select count(c.id) num,
                  |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then c.id end) num_week,
                  |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and share_time<'${DATENOW}' then c.id end) month_week,
                  |    c.num_type num_type,subject_id,stage_id
                  |from (
                  |    select ec.*,
                  |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                  |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                  |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                  |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                  |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                  |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                  |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                  |    when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                  |             else 0 end num_type
                  |    from neworiental_v3.entity_res_clip ec
                  |    where ec.platform_check_status=0 and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                  |where c.num_type!=0
                  |group by c.subject_id,c.stage_id,c.num_type
                  |union all
                  |select count(distinct t.num_id) num,
                  |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                  |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                  |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                  |(
                  |select 1 type,lerm.org_id code_id,2 org_type,eq.id num_id,upload_time,
                  |   6 num_type,esb.id subject_id,esb.stage_id stage_id
                  |from neworiental_v3.entity_question eq
                  |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                  |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                  |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                  |where lerm.org_type=2 and eq.org_type=2 and lerm.state=1 and eq.org_id=lerm.org_id
                  |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                  |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                  |)t
                  |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                  |union all
                  |select count(c.id) num,
                  |    count(case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then c.id end) num_week,
                  |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and share_time<'${DATENOW}' then c.id end) month_week ,
                  |    11 num_type,subject_id,stage_id
                  |from neworiental_v3.entity_exercise c
                  |where platform_check_status=0 and subject_id is not null and stage_id is not null and status=1
                  |and category in (30,31,32)
                  |group by subject_id,stage_id
                  |union all
                  |select count(c.id) num,
                  |    count(case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then c.id end) num_week,
                  |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and share_time<'${DATENOW}' then c.id end) month_week ,
                  |    12 num_type,subject_id,stage_id
                  |from neworiental_v3.entity_course_package c
                  |where  platform_check_status=0 and subject_id is not null and stage_id is not null and status=1
                  |group by subject_id,stage_id """.stripMargin
    sql(str_all)
    sc_1.stop()
  }
}
