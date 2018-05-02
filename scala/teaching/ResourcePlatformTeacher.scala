package teaching

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by ztf on 2017/10/29.
  */
object ResourcePlatformTeacher {
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
      val str_kejian=s""" insert into table test.resource_school_tmp
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
                       |and c.stage_id is not null   and c.status=1 and lerm.state=1 and c.org_id=lerm.org_id and
                       |(c.platform_check_status=0 or  c.school_check_status=0)
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
                       |and  lerm.state=1 and  d.region_code=lerm.area_code and (c.area_check_status=0 or  c.platform_check_status=0)
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
                       |and  lerm.state=1 and d.region_code=lerm.city_code and  (c.city_check_status=0 or c.platform_check_status=0)
                       |
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
                       |and  lerm.state=1 and d.region_code=lerm.province_code and (c.province_check_status=0 or c.platform_check_status=0)
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id """.stripMargin
      sql(str_kejian)
      val str_ziliao=s""" insert into table test.resource_school_tmp
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
                       |    when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |    from neworiental_v3.entity_res_clip ec
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_school lerm on lerm.resource_mark_id=erm.id
                       |where ec.org_type=2 and lerm.org_type=2 and  lerm.state=1 and ec.org_id=lerm.org_id and (ec.school_check_status=0 or  ec.platform_check_status=0)
                       |and erm.resource_type=3 and ec.subject_id is not null and ec.stage_id is not null and ec.status=1 ) c where c.num_type!=0
                       |)t
                       |group by t.code_id,t.org_type,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |union all
                       |
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
                       |      when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |    from neworiental_v3.entity_res_clip ec
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                       |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                       |where  lerm.state=1 and d.code=ec.area_code and (ec.area_check_status=0 or ec.platform_check_status=0 )
                       |and erm.resource_type=3 and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                       |where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |
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
                       |where lerm.state=1 and ec.city_code=d.code and (ec.city_check_status=0 or ec.platform_check_status=0)
                       |and  erm.resource_type=3  and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                       |where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type
                       |--新增 结束
                       |
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
                       |       when (ec.ext_id=0 or ec.ext_id is null) and ec.resource_tag=6 then 14
                       |             else 0 end num_type
                       |    from neworiental_v3.entity_res_clip ec
                       |join neworiental_v3.entity_resource_mark erm on erm.resource_id=ec.id
                       |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                       |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                       |where lerm.state=1  and ec.province_code=d.code and (ec.province_check_status=0 or ec.platform_check_status=0)
                       |and ec.subject_id is not null and ec.stage_id is not null and ec.status=1) c
                       |where c.num_type!=0
                       |)t
                       |group by t.code_id,t.subject_id,t.stage_id,t.num_type """.stripMargin
      sql(str_ziliao)
      val str_xiti=s""" insert into table test.resource_school_tmp
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
                     |where lerm.org_type=2 and ee.org_type=2 and lerm.state=1  and ee.org_id=lerm.org_id  and  (ee.school_check_status=0 or ee.platform_check_status=0)
                     |and  erm.resource_type=2 and ee.category in (30,31,32) and subject_id is not null and stage_id is not null and ee.status=1
                     |)t
                     |group by t.code_id,t.org_type,t.subject_id,t.stage_id
                     |--新增 结束
                     |union all
                     |
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
                     |where  lerm.state=1 and d.code=ee.area_code and (ee.area_check_status=0 or ee.platform_check_status=0)
                     |and erm.resource_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                     |)t
                     |group by t.code_id,t.subject_id,t.stage_id
                     |--新增 结束
                     |
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
                     |where lerm.state=1 and d.code=ee.city_code and (ee.city_check_status=0 or  ee.platform_check_status=0)
                     |and erm.resource_type=2  and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                     |)t
                     |group by t.code_id,t.subject_id,t.stage_id
                     |--新增 结束
                     |
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
                     |where lerm.state=1 and ee.province_code=d.code and (ee.province_check_status=0 or ee.platform_check_status=0)
                     |and erm.resource_type=2 and ee.category in (30,31,32) and ee.subject_id is not null and ee.stage_id is not null and ee.status=1
                     |)t
                     |group by t.code_id,t.subject_id,t.stage_id """.stripMargin
      sql(str_xiti)
      val str_kechengbao=s""" insert into table test.resource_school_tmp
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
                           |where lerm.org_type=2 and ecp.org_type=2 and lerm.state=1 and ecp.org_id=lerm.org_id and (ecp.school_check_status=0 or ecp.platform_check_status=0 )
                           |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
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
                           |where lerm.state=1 and d.code=ecp.area_code and (ecp.school_check_status=0 or ecp.platform_check_status=0)
                           |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
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
                           |where lerm.state=1 and ecp.city_code=d.code and (ecp.school_check_status=0 or ecp.platform_check_status=0)
                           |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
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
                           |where lerm.state=1 and d.code=ecp.province_code and (ecp.school_check_status=0 or ecp.platform_check_status=0)
                           |and  erm.resource_type=5 and subject_id is not null and stage_id is not null and ecp.status=1
                           |)t
                           |group by t.code_id,t.org_type,t.subject_id,t.stage_id """.stripMargin
      sql(str_kechengbao)
      val str_tiku=s""" insert into table test.resource_school_tmp
                     |select 1 type,t.code_id code_id,2 org_type,count(distinct t.num_id) num,
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
                     |select 2 type,t.code_id code_id,0 org_type,count(distinct t.num_id) num,
                     |    count(distinct case when upload_time>=date_sub(date_sub('${DATE}',1),pmod(datediff(date_sub('${DATE}',1),'1920-01-01')-3,7)-1) and upload_time<'${DATENOW}' then  t.num_id end) num_week,
                     |	count( distinct  case when t.upload_time>=date_sub(date_sub('${DATE}',1),day('${DATE}')-2) and t.upload_time<'${DATENOW}' then t.num_id end) month_week,
                     |    6 num_type,t.subject_id subject_id,t.stage_id stage_id from
                     |(
                     |select 2 type,d.code code_id,0 org_type,eq.id num_id,upload_time,
                     |    6 num_type,esb.id subject_id,esb.stage_id stage_id
                     |from neworiental_v3.entity_question eq
                     |join neworiental_report.code_region cr on cr.org_id=eq.org_id
                     |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                     |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                     |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                     |join neworiental_v3.entity_public_director d on d.region_code=lerm.area_code
                     |where  eq.org_type=2 and cr.public_private=2 and lerm.state=1 and d.code=cr.area_code
                     |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
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
                     |join neworiental_report.code_region cr on cr.org_id=eq.org_id
                     |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                     |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                     |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                     |join neworiental_v3.entity_public_director d on d.region_code=lerm.city_code
                     |where  eq.org_type=2 and cr.public_private=2 and lerm.state=1 and d.code=cr.city_code
                     |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
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
                     |join neworiental_report.code_region cr on cr.org_id=eq.org_id
                     |join neworiental_v3.entity_subject esb  on eq.subject_id=esb.id
                     |join neworiental_v3.entity_resource_mark erm on erm.resource_id=eq.id
                     |join neworiental_v3.link_entity_resource_mark_region lerm on lerm.resource_mark_id=erm.id
                     |join neworiental_v3.entity_public_director d on d.region_code=lerm.province_code
                     |where  eq.org_type=2 and cr.public_private=2 and lerm.state=1 and d.code=cr.province_code
                     |and  erm.resource_type=4 and subject_id is not null and stage_id is not null and
                     |eq.new_format=1 and eq.state='ENABLED' and eq.parent_question_id=0
                     |)t
                     |group by t.code_id,t.org_type,t.subject_id,t.stage_id """.stripMargin
      sql(str_tiku)
      sc_1.stop()

    }
}
