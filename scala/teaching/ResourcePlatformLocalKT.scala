package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/11/1.
  */
object ResourcePlatformLocalKT {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("HiveAnswerSpark")
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
    val DATE = args(0).toString
    val DATENOW = args(1).toString
    val str_kechengbao=s"""  insert into table test.resource_local_tmp
                          |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
                          |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then  t.num_id end) num_week,
                          |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                          |    12 num_type,t.subject_id subject_id,t.stage_id stage_id from
                          |(
                          |select 1 type,lerm.org_id code_id,2 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from neworiental_v3.entity_course_package ecp
                          |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ecp.id
                          |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                          |where lerm.org_type=2 and ecp.org_type=2 and lerm.state=1
                          |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
                          |--初始化资源库
                          |union all
                          |select 1 type,prv.org_id code_id,2 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |	from  (select org_id,2 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                          |lateral view explode(split(version_ids,',')) t as version_id
                          |where director_type=1) prv
                          |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                          |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                          |join  neworiental_v3.entity_course_package ecp on ecp.chapter_id=etc.id
                          |where prv.org_type=2 and ecp.org_type=2 and ecp.subject_id is not null and ecp.stage_id is not null and ecp.status=1
                          |)t
                          |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                          |union all
                          |select 2 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                          |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then  t.num_id end) num_week,
                          |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                          |    12 num_type,t.subject_id subject_id,t.stage_id stage_id from
                          |(
                          |select 2 type,d.code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from neworiental_v3.entity_course_package ecp
                          |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ecp.id
                          |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                          |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                          |where lerm.state=1
                          |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
                          |--初始化资源库
                          |union all
                          |select 2 type,prv.director_code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from  (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                          |lateral view explode(split(version_ids,',')) t as version_id
                          |where director_type=2) prv
                          |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                          |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                          |join  neworiental_v3.entity_course_package ecp on ecp.chapter_id=etc.id
                          |where ecp.subject_id is not null and ecp.stage_id is not null and ecp.status=1
                          |)t
                          |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                          |union all
                          |select 3 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                          |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then  t.num_id end) num_week,
                          |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                          |    12 num_type,t.subject_id subject_id,t.stage_id stage_id from
                          |(
                          |select 3 type,d.code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from neworiental_v3.entity_course_package ecp
                          |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ecp.id
                          |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                          |join neworiental_v3.entity_public_director d on d.region_code=lerm.city_code
                          |where lerm.state=1
                          |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
                          |--初始化资源库
                          |union all
                          |select 3 type,prv.director_code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from  (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                          |lateral view explode(split(version_ids,',')) t as version_id
                          |where director_type=3) prv
                          |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                          |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                          |join  neworiental_v3.entity_course_package ecp on ecp.chapter_id=etc.id
                          |where ecp.subject_id is not null and ecp.stage_id is not null and ecp.status=1
                          |)t
                          |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                          |union all
                          |select 4 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                          |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then  t.num_id end) num_week,
                          |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                          |    12 num_type,t.subject_id subject_id,t.stage_id stage_id from
                          |(
                          |select 4 type,d.code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from neworiental_v3.entity_course_package ecp
                          |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ecp.id
                          |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                          |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                          |where lerm.state=1
                          |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
                          |--初始化资源库
                          |union all
                          |select 4 type,prv.director_code code_id,0 org_type,ecp.id num_id,share_time,
                          |    12 num_type,ecp.subject_id subject_id,ecp.stage_id stage_id
                          |from  (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                          |lateral view explode(split(version_ids,',')) t as version_id
                          |where director_type=4) prv
                          |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                          |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                          |join  neworiental_v3.entity_course_package ecp on ecp.chapter_id=etc.id
                          |where ecp.subject_id is not null and ecp.stage_id is not null and ecp.status=1
                          |)t
                          |group by t.code_id,t.org_type,t.subject_id,t.stage_id """.stripMargin
    sql(str_kechengbao)
    val str_tiku=s""" insert into table test.resource_local_tmp
                    |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                    |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                    |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(
                    |select 1 type,lerm.org_id code_id,2 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from neworiental_v3.entity_question eq
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                    |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                    |where lerm.org_type=2 and eq.org_type=2 and lerm.state=1
                    |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |--初始化资源库
                    |union all
                    |select 1 type,prv.org_id code_id,2 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from
                    |(select org_id,2 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=1 ) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.link_question_chapter lqp on lqp.chapter_id=etc.id
                    |join neworiental_v3.entity_question eq  on eq.id=lqp.question_id
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |where  eq.org_type=2 and eq.subject_id is not null and esb.stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |)t
                    |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                    |union all
                    |select 2 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                    |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                    |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(
                    |select 2 type,d.code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from neworiental_v3.entity_question eq
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                    |where  eq.org_type=2 and lerm.state=1
                    |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |--初始化资源库
                    |union all
                    |select 2 type,prv.director_code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from
                    |(select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=2) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.link_question_chapter lqp on lqp.chapter_id=etc.id
                    |join neworiental_v3.entity_question eq  on eq.id=lqp.question_id
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |where eq.org_type=2  and eq.subject_id is not null and esb.stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |)t
                    |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                    |union all
                    |select 3 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                    |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                    |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(
                    |select 3 type,d.code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from neworiental_v3.entity_question eq
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.city_code
                    |where  eq.org_type=2 and lerm.state=1
                    |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |--初始化资源库
                    |union all
                    |select 3 type,prv.director_code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from
                    |(select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=3) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.link_question_chapter lqp on lqp.chapter_id=etc.id
                    |join neworiental_v3.entity_question eq  on eq.id=lqp.question_id
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |where eq.org_type=2  and eq.subject_id is not null and esb.stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |)t
                    |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                    |union all
                    |select 4 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                    |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                    |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(
                    |select 4 type,d.code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from neworiental_v3.entity_question eq
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                    |where  eq.org_type=2 and lerm.state=1
                    |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |--初始化资源库
                    |union all
                    |select 4 type,prv.director_code code_id,0 org_type,eq.id num_id,upload_time,
                    |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                    |from
                    |(select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=4) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.link_question_chapter lqp on lqp.chapter_id=etc.id
                    |join neworiental_v3.entity_question eq  on eq.id=lqp.question_id
                    |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                    |where eq.org_type=2  and eq.subject_id is not null and esb.stage_id is not null and
                    |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                    |)t
                    |group by t.code_id,t.org_type,t.subject_id,t.stage_id """.stripMargin
    sql(str_tiku)
    sc_1.stop()
  }
}
