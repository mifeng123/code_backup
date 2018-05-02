package teaching

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ztf on 2017/10/28.
  */
object ResourcePlatformLocalKZX {
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
    val str_kejian=s""" --课件
                      |insert into table test.resource_local_tmp
                      |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
                      |    count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then t.num_id end) num_week,
                      |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                      |    1 num_type,t.subject_id subject_id,t.stage_id stage_id from
                      |(
                      |select 1 type,lerm.org_id code_id,2 org_type,c.id  num_id,share_time,
                      |    1 num_type,c.subject_id subject_id,c.stage_id stage_id
                      |from neworiental_v3.entity_res_courseware c
                      |join neworiental_v3.entity_resource_mark erm on c.id=erm.resource_id
                      |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                      |where   lerm.org_type=2 and c.org_type=2 and erm.resource_type=1 and c.subject_id is not null
                      |and c.stage_id is not null   and c.status=1 and lerm.state=1
                      |--初始化资源库
                      |union all
                      |select 1 type, prv.org_id code_id,2 org_type,c.id num_id,share_time,1 num_type,prv.subject_id subject_id,
                      |prv.stage_id stage_id from
                      |(select org_id,2 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                      |lateral view explode(split(version_ids,',')) t as version_id
                      |where director_type=1) prv
                      |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                      |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                      |join  neworiental_v3.entity_res_courseware c on c.chapter_id=etc.id
                      |where c.status=1
                      |)t
                      |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                      |--新增结束
                      |union all
                      |select 2 type,t.code_id code_id,0 org_type, count(distinct t.num_id) num,
                      |  count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then t.num_id end) num_week,
                      |  count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                      |    1 num_type,t.subject_id subject_id,t.stage_id stage_id from
                      |(select 2 type,d.code code_id,0 org_type, c.id  num_id,share_time,
                      |    1 num_type,c.subject_id subject_id,stage_id
                      |from neworiental_v3.entity_res_courseware c
                      |join neworiental_v3.entity_resource_mark erm on c.id=erm.resource_id
                      |join neworiental_v3.link_entity_resource_mark_region  lerm on lerm.resource_mark_id=erm.id
                      |join neworiental_v3.entity_public_director d on c.area_code=d.code
                      |where   erm.resource_type=1 and c.subject_id is not null and stage_id is not null and c.status=1
                      |and  lerm.state=1
                      |--初始化资源库
                      |union all
                      |select 2 type, prv.director_code code_id,0 org_type,c.id num_id,share_time,1 num_type,prv.subject_id subject_id,
                      |prv.stage_id stage_id from
                      |(select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                      |lateral view explode(split(version_ids,',')) t as version_id
                      |where director_type=2) prv
                      |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                      |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                      |join  neworiental_v3.entity_res_courseware c on c.chapter_id=etc.id
                      |where c.status=1
                      |)t
                      |group by t.code_id,t.subject_id,t.stage_id
                      |--新增  结束
                      |union all
                      |select 3 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                      |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then  t.num_id end) num_week,
                      |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                      |    1 num_type,t.subject_id subject_id,t.stage_id stage_id from
                      |(
                      |select 3 type,d.code code_id,0 org_type, c.id  num_id,share_time,
                      |    1 num_type,c.subject_id subject_id,stage_id
                      |from neworiental_v3.entity_res_courseware c
                      |join neworiental_v3.entity_resource_mark erm on erm.resource_id=c.id
                      |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                      |join neworiental_v3.entity_public_director d on c.city_code=d.code
                      |where erm.resource_type=1 and c.subject_id is not null and stage_id is not null and c.status=1
                      |and  lerm.state=1
                      |--初始化资源库
                      |union all
                      |select 3 type, prv.director_code code_id,0 org_type,c.id num_id,share_time,1 num_type,prv.subject_id subject_id,
                      |prv.stage_id stage_id from
                      |(select director_code ,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                      |lateral view explode(split(version_ids,',')) t as version_id
                      |where director_type=3) prv
                      |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                      |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                      |join  neworiental_v3.entity_res_courseware c on c.chapter_id=etc.id
                      |where c.status=1
                      |)t
                      |group by t.code_id,t.subject_id,t.stage_id
                      |--新增 结束
                      |union all
                      |select 4 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                      |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then   t.num_id end) num_week,
                      |    count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                      |    1 num_type,t.subject_id subject_id ,t.stage_id stage_id from
                      |(
                      |select 4 type, d.code code_id,0 org_type, c.id  num_id,share_time,
                      |    1 num_type,c.subject_id subject_id,c.stage_id stage_id
                      |from neworiental_v3.entity_res_courseware c
                      |join neworiental_v3.entity_resource_mark erm on erm.resource_id=c.id
                      |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                      |join neworiental_v3.entity_public_director d on c.province_code=d.code
                      |where   erm.resource_type=1 and c.subject_id is not null and c.stage_id is not null and c.status=1
                      |and  lerm.state=1
                      |--初始化资源库
                      |union all
                      |select 4 type, prv.director_code code_id,0 org_type,c.id num_id,share_time,1 num_type,prv.subject_id subject_id,
                      |prv.stage_id  stage_id from
                      |(select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                      |lateral view explode(split(version_ids,',')) t as version_id
                      |where director_type=4) prv
                      |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                      |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                      |join  neworiental_v3.entity_res_courseware c on c.chapter_id=etc.id
                      |where c.status=1
                      |)t
                      |group by t.code_id,t.subject_id,t.stage_id """.stripMargin
    sql(str_kejian)
    val  str_ziliao=s""" insert into table test.resource_local_tmp
                       |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
                       |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then  t.num_id end) num_week,
                       |    count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                       |    t.num_type num_type,t.subject_id subject_id,t.stage_id stage_id from
                       |(
                       |select 1 type,c.code_id code_id,2 org_type,c.id num_id,share_time,
                       |    c.num_type  num_type,c.subject_id subject_id,c.stage_id stage_id
                       |from (
                       |    select ec.*,lerm.org_id code_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |       when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |    from neworiental_v3.entity_res_clip ec
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                       |where ec.org_type=2 and lerm.org_type=2 and  lerm.state=1
                       |and erm.resource_type=3 and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c where c.num_type!=0
                       |--初始化资源库
                       |union all
                       |select 1 type,c.code_id code_id,2 org_type,c.id num_id,share_time,
                       |    c.num_type  num_type,c.ject_id subject_id,c.tag_id stage_id
                       |from (
                       |    select ec.*,prv.org_id code_id,2 type_id,prv.stage_id tag_id,prv.subject_id ject_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |       when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |   from (select org_id,2 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                       |lateral view explode(split(version_ids,',')) t as version_id
                       |where director_type=1) prv
                       |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                       |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                       |join  neworiental_v3.entity_res_clip ec on ec.chapter_id=etc.id
                       |where ec.status=1) c where c.num_type!=0
                       |)t
                       |group by t.code_id,t.org_type,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |union all
                       |select 2 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                       |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then   t.num_id end) num_week,
                       |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                       |    t.num_type num_type,t.subject_id subject_id,t.stage_id stage_id from
                       |(
                       |select 2 type,c.code_id code_id,0 org_type, c.id  num_id,share_time,
                       |    c.num_type num_type,c.subject_id subject_id,c.stage_id stage_id
                       |from (
                       |    select ec.*,d.code code_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |       when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |    from neworiental_v3.entity_res_clip ec
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                       |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                       |where  lerm.state=1
                       |and erm.resource_type=3 and ec.subject_id is not null and ec.stage_id is not null and ec.status=1 ) c
                       |where c.num_type!=0
                       |--初始化资源库
                       |union all
                       |select 2 type,c.code_id code_id,0 org_type,c.id num_id,share_time,
                       |    c.num_type  num_type,c.ject_id subject_id,c.tag_id stage_id
                       |from (
                       |    select ec.*,prv.director_code code_id,0 type_id,prv.stage_id tag_id,prv.subject_id ject_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |    when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |   from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                       |lateral view explode(split(version_ids,',')) t as version_id
                       |where director_type=2) prv
                       |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                       |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                       |join  neworiental_v3.entity_res_clip ec on ec.chapter_id=etc.id
                       |where ec.status=1) c where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |union all
                       |select 3 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                       |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then   t.num_id end) num_week,
                       |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                       |    t.num_type num_type,t.subject_id subject_id,t.stage_id stage_id from
                       |(
                       |select 3 type, c.code_id code_id,0 org_type,c.id num_id,share_time,
                       |    c.num_type num_type,c.subject_id subject_id,c.stage_id stage_id
                       |from (
                       |    select ec.*,d.code code_id,
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
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                       |join neworiental_v3.entity_public_director d on d.region_code=lerm.city_code
                       |where lerm.state=1
                       |and  erm.resource_type=3  and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                       |where c.num_type!=0
                       |--初始化资源库
                       |union all
                       |select 3 type,c.code_id code_id,0 org_type,c.id num_id,share_time,
                       |    c.num_type  num_type,c.ject_id subject_id,c.tag_id stage_id
                       |from (
                       |    select ec.*,prv.director_code code_id,0 type_id,prv.stage_id tag_id,prv.subject_id ject_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |    when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |   from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                       |lateral view explode(split(version_ids,',')) t as version_id
                       |where director_type=3) prv
                       |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                       |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                       |join  neworiental_v3.entity_res_clip ec on ec.chapter_id=etc.id
                       |where ec.status=1) c where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |union all
                       |select 4 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                       |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then   t.num_id end) num_week,
                       |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                       |    t.num_type num_type,t.subject_id subject_id,t.stage_id stage_id from
                       |(select 4 type, c.code_id code_id,0 org_type,c.id num_id,share_time,
                       |    c.num_type num_type,c.subject_id subject_id,c.stage_id stage_id
                       |from (
                       |    select ec.*,d.code code_id,
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
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                       |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                       |where lerm.state=1
                       |and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                       |where c.num_type!=0
                       |--初始化资源库
                       |union all
                       |select 4 type,c.code_id code_id,0 org_type,c.id num_id,share_time,
                       |    c.num_type  num_type,c.ject_id subject_id,c.tag_id stage_id
                       |from (
                       |    select ec.*,prv.director_code code_id,0 type_id,prv.stage_id tag_id,prv.subject_id ject_id,
                       |        case when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=2 then 2
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=3 then 3
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=4 then 4
                       |             when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=5 then 5
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=7 then 7
                       |			 when ec.ext_id>0 and ec.resource_tag in (5,9) then 9
                       |			 when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=11 then 13
                       |    when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |   from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                       |lateral view explode(split(version_ids,',')) t as version_id
                       |where director_type=4) prv
                       |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                       |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                       |join  neworiental_v3.entity_res_clip ec on ec.chapter_id=etc.id
                       |where ec.status=1) c where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type """.stripMargin
    sql(str_ziliao)
    val str_xiti=s""" insert into table test.resource_local_tmp
                    |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and share_time<'${DATENOW}' then  t.num_id end) num_week,
                    |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                    |    11 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(
                    |select 1 type,lerm.org_id code_id,2 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |from neworiental_v3.entity_exercise ee
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ee.id
                    |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                    |where lerm.org_type=2 and ee.org_type=2 and lerm.state=1
                    |and  erm.resource_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |--初始化资源库
                    |union all
                    |select 1 type,prv.org_id code_id,2 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |	from (select org_id,2 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=1) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.entity_exercise ee on ee.chapter_id=etc.id
                    |where ee.org_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |)t
                    |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                    |--新增 结束
                    |union all
                    |select 2 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then   t.num_id end) num_week,
                    |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                    |    11 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(select 2 type,d.code code_id,0 org_type,ee.id  num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |from neworiental_v3.entity_exercise ee
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ee.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                    |where  lerm.state=1
                    |and erm.resource_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |--初始化资源库
                    |union all
                    |select 2 type,prv.director_code code_id,0 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |	from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=2) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.entity_exercise ee on ee.chapter_id=etc.id
                    |where ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |)t
                    |group by t.code_id,t.subject_id,t.stage_id
                    |--新增 结束
                    |union all
                    |select 3 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then   t.num_id end) num_week,
                    |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                    |    11 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(select 3 type, d.code code_id,0 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |from neworiental_v3.entity_exercise ee
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ee.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.city_code
                    |where lerm.state=1
                    |and erm.resource_type=2  and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |--初始化资源库
                    |union all
                    |select 3 type,prv.director_code code_id,0 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |	from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=3) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.entity_exercise ee on ee.chapter_id=etc.id
                    |where ee.org_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |)t
                    |group by t.code_id,t.subject_id,t.stage_id
                    |--新增 结束
                    |union all
                    |select 4 type, t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                    |    count(distinct case when t.share_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and t.share_time<'${DATENOW}' then   t.num_id end) num_week,
                    |	count( distinct  case when t.share_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.share_time<'${DATENOW}' then t.num_id end) month_week,
                    |    11 num_type,t.subject_id subject_id,t.stage_id stage_id from
                    |(select 4 type, d.code code_id,0 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |from neworiental_v3.entity_exercise ee
                    |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ee.id
                    |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                    |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                    |where lerm.state=1
                    |and erm.resource_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |--初始化资源库
                    |union all
                    |select 4 type,prv.director_code code_id,0 org_type,ee.id num_id,share_time,
                    |    11 num_type,ee.subject_id subject_id,ee.stage_id stage_id
                    |	from (select director_code,0 org_type,stage_id,subject_id,version_id from  test.link_resource_platform_version_config
                    |lateral view explode(split(version_ids,',')) t as version_id
                    |where director_type=4) prv
                    |join  neworiental_v3.entity_teaching_directory etd on etd.version_id =prv.version_id
                    |join  neworiental_v3.entity_teaching_chapter etc on etc.directory_id=etd.id
                    |join  neworiental_v3.entity_exercise ee on ee.chapter_id=etc.id
                    |where  ee.org_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                    |)t
                    |group by t.code_id,t.subject_id,t.stage_id """.stripMargin
    sql(str_xiti)
    sc_1.stop()
  }

}
