package TeacherSpace

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object EntityResourceUse {
def main(args: Array[String]): Unit = {
  var sparkConf = new SparkConf()
    .setAppName("TeacherSpace")
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

    val DAY = args(0).toString
    val DATE = args(1).toString
   val DATENOW = args(2).toString
    /*val DAY6AGO = args(4).toString*/
  /*课件*/
  val  str_drop_coeru=""" drop table  test.courseware_entity_resource_use """.stripMargin
  sql(str_drop_coeru)
  val str_create_coeru=s""" create table test.courseware_entity_resource_use as
                        |select distinct  1 resource_type,tmp_1.resource_id,tmp_1.op_time,tmp_1.op_type from
                        |(select tmp.resource_id,tmp.op_time op_time,tmp.op_type
                        |,row_number() over (partition by tmp.resource_id order by tmp.op_time desc) rank
                        |from
                        |(select   erc.id resource_id,
                        |case when erc.update_time>erc.submit_time then  erc.update_time
                        | else erc.submit_time end op_time
                        |,1 op_type
                        | from neworiental_v3.entity_res_courseware erc
                        |where (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--收藏
                        |select   lclr.resource_id,lclr.create_time op_time,2 op_type from neworiental_v3.entity_res_courseware erc
                        |join neworiental_v3.link_custom_list_resource lclr on lclr.resource_id=erc.id
                        |where  lclr.is_fav=1 and lclr.resource_type=1
                        |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--引用
                        |select  lrr.resource_id,lrr.create_time op_time,3 op_type from neworiental_v3.entity_res_courseware erc
                        |join neworiental_v3.link_respackage_resource lrr on lrr.resource_id=erc.id
                        |where  lrr.resource_type=1
                        |and  (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--发布
                        |select  lrpr.resource_id,lrpr.create_time op_time,4 op_type from neworiental_v3.entity_res_courseware erc
                        |join neworiental_v3.link_respackage_publish_resource  lrpr on lrpr.resource_id=erc.id
                        |where  lrpr.resource_type=1
                        |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |) tmp
                        |) tmp_1 where tmp_1.rank=1  and to_date(tmp_1.op_time)='${DATE}'
                        | and to_date(tmp_1.op_time)<'${DATENOW}'
                        | """.stripMargin
  sql(str_create_coeru)
  /*题集*/
  val str_drop_eeru=""" drop table test.exercise_entity_resource_use """.stripMargin
  sql(str_drop_eeru)
  val str_create_eeru=s""" create table test.exercise_entity_resource_use as
                        |select distinct  2 resource_type,tmp_1.resource_id,tmp_1.op_time,tmp_1.op_type from
                        |(select tmp.resource_id,tmp.op_time op_time,tmp.op_type
                        |,row_number() over (partition by tmp.resource_id order by tmp.op_time desc) rank
                        |from
                        |(select   erc.id resource_id,
                        |case when erc.update_time>erc.submit_time then  erc.update_time
                        | else erc.submit_time end op_time
                        |,1 op_type
                        | from neworiental_v3.entity_exercise erc
                        |where erc.update_time!='null' and
                        |(erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--收藏
                        |select   lclr.resource_id,lclr.create_time op_time,2 op_type from neworiental_v3.entity_exercise erc
                        |join neworiental_v3.link_custom_list_resource lclr on lclr.resource_id=erc.id
                        |where  lclr.is_fav=1 and lclr.resource_type=2
                        |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--引用
                        |select  lrr.resource_id,lrr.create_time op_time,3 op_type from neworiental_v3.entity_exercise erc
                        |join neworiental_v3.link_respackage_resource lrr on lrr.resource_id=erc.id
                        |where  lrr.resource_type=2
                        |and  (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |union ALL
                        |--发布
                        |select  lrpr.resource_id,lrpr.create_time op_time,4 op_type from neworiental_v3.entity_exercise erc
                        |join neworiental_v3.link_respackage_publish_resource  lrpr on lrpr.resource_id=erc.id
                        |where  lrpr.resource_type=2
                        |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                        |) tmp
                        |) tmp_1 where tmp_1.rank=1  and to_date(tmp_1.op_time)>='${DATE}'
                        | and to_date(tmp_1.op_time)<'${DATENOW}'
                        | """.stripMargin
  sql(str_create_eeru)
  /*资料*/
  val str_drop_cleru=""" drop table test.clip_entity_resource_use """.stripMargin
  sql(str_drop_cleru)
  val  str_create_cleru=s""" create table test.clip_entity_resource_use as
                          |select distinct  3 resource_type,tmp_1.resource_id,tmp_1.op_time,tmp_1.op_type from
                          |(select tmp.resource_id,tmp.op_time op_time,tmp.op_type
                          |,row_number() over (partition by tmp.resource_id order by tmp.op_time desc) rank
                          |from
                          |(select   erc.id resource_id,
                          |case when erc.update_time>erc.submit_time then  erc.update_time
                          | else erc.submit_time end op_time
                          |,1 op_type
                          | from neworiental_v3.entity_res_clip erc
                          |where (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                          |union ALL
                          |--收藏
                          |select   lclr.resource_id,lclr.create_time op_time,2 op_type from neworiental_v3.entity_res_clip erc
                          |join neworiental_v3.link_custom_list_resource lclr on lclr.resource_id=erc.id
                          |where  lclr.is_fav=1 and lclr.resource_type=3
                          |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                          |union ALL
                          |--引用
                          |select  lrr.resource_id,lrr.create_time op_time,3 op_type from neworiental_v3.entity_res_clip erc
                          |join neworiental_v3.link_respackage_resource lrr on lrr.resource_id=erc.id
                          |where  lrr.resource_type=3
                          |and  (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                          |union ALL
                          |--发布
                          |select  lrpr.resource_id,lrpr.create_time op_time,4 op_type from neworiental_v3.entity_res_clip erc
                          |join neworiental_v3.link_respackage_publish_resource  lrpr on lrpr.resource_id=erc.id
                          |where  lrpr.resource_type=3
                          |and (erc.platform_check_status=0 or erc.province_check_status=0 or erc.city_check_status=0 or erc.area_check_status=0 or erc.school_check_status=0) and erc.`status`=1
                          |) tmp
                          |) tmp_1 where tmp_1.rank=1 and to_date(tmp_1.op_time)='${DATE}'
                          |and to_date(tmp_1.op_time)<'${DATENOW}'
                          |""".stripMargin
  sql(str_create_cleru)

  /*最终表*/
  val str_drop_eru=""" drop table test.entity_resource_use """.stripMargin
  sql(str_drop_eru)
  val str_create_eru=s"""  create table test.entity_resource_use as
                       |select cast(concat('${DAY}',row_number() over()) as bigint) id,tmp_l.*, '${DATE}' time  from
                       |(select distinct tmp.* from
                       |(
                       |select  distinct  resource_type,resource_id,op_type last_op,op_time  from test.courseware_entity_resource_use
                       |union all
                       |select  distinct  resource_type,resource_id,op_type last_op,op_time  from test.exercise_entity_resource_use
                       |union all
                       |select  distinct  resource_type,resource_id,op_type last_op,op_time  from test.clip_entity_resource_use
                       |) tmp) tmp_l """.stripMargin
  sql(str_create_eru)
  sc_1.stop()
}
}
