package resource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object finishedProduct {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("finishedProduct")
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
    /*val DAYNOW = args(1).toString
    val DATE = args(2).toString
    val DATENOW = args(3).toString*/
   /*资料成品   test.xuli_list  虚拟账号表*/
    val str_drop_zl="""  drop table test.clip_finished_product  """.stripMargin
    sql(str_drop_zl)
    val str_create_zl=s""" create table test.clip_finished_product as
                         |select  distinct (case when t.xuni_id is not null or t.org_id is  not null then t.id end) as id, 3  as  resource_type ,"${DATE}" as time
                         |  from (
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_clip c
                         |					left join neworiental_user.entity_user u on u.system_id=c.created_by
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=c.created_by
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and s.enable=1 and u.org_type=2 and
                         |					(to_date(c.submit_time) = "${DATE}"  or to_date(c.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_clip c
                         |					join neworiental_v3.link_custom_list_resource r on r.resource_id=c.id
                         |					left join neworiental_user.entity_user u on u.system_id=r.system_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=c.created_by
                         |					where c.is_finished_product!=1 and c.status=1 and r.is_fav=1 and s.private=0 and s.enable=1 and r.resource_type=3 and u.org_type=2
                         |					and (to_date(r.create_time) = "${DATE}" 	or to_date(r.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_clip c
                         |					join neworiental_v3.link_respackage_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage r on r.id=l.package_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and   l.resource_type=3 and u.org_type=2  and s.enable=1
                         |					and to_date(r.create_time) = "${DATE}"
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_clip c
                         |					join neworiental_v3.link_respackage_publish_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage_publish r on r.id=l.publish_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and s.enable=1 and l.resource_type=3 and u.org_type=2
                         |					and  to_date(r.create_time)  = "${DATE}"
                         |
                         |)t  """.stripMargin
    sql(str_create_zl)
    /*课件成品*/
    val  str_drop_kj=""" drop table test.courseware_finished_product """.stripMargin
    sql(str_drop_kj)
    val str_create_kj=s"""  create table  test.courseware_finished_product as
                         |select  distinct (case when t.xuni_id is not null   or t.org_id is  not null then t.id end) as id, 1  as  resource_type ,"${DATE}" as time
                         |from
                         |(
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_courseware c
                         |					left join neworiental_user.entity_user u on u.system_id=c.created_by
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=c.created_by
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and s.enable=1 and u.org_type=2 and
                         |					(to_date(c.submit_time) = "${DATE}" or to_date(c.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_courseware c
                         |					join neworiental_v3.link_custom_list_resource r on r.resource_id=c.id
                         |					left join neworiental_user.entity_user u on u.system_id=r.system_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.system_id
                         |					where c.is_finished_product!=1 and c.status=1 and r.is_fav=1 and s.private=0 and s.enable=1 and r.resource_type=1 and u.org_type=2
                         |					and (to_date(r.create_time) = "${DATE}" or  to_date(r.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_courseware c
                         |					join neworiental_v3.link_respackage_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage r on r.id=l.package_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and   l.resource_type=1 and u.org_type=2 and s.enable=1
                         |					and to_date(r.create_time) = "${DATE}"
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id from neworiental_v3.entity_res_courseware c
                         |					join neworiental_v3.link_respackage_publish_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage_publish r on r.id=l.publish_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and l.resource_type=1 and u.org_type=2 and  s.enable=1
                         |					and  to_date(r.create_time) = "${DATE}"
                         |)t  """.stripMargin
    sql(str_create_kj)
    /*题集成品*/
    val str_drop_tj=""" drop table test.exercise_finished_product """.stripMargin
    sql(str_drop_tj)
    val str_create_tj=s""" create table test.exercise_finished_product as
                         |select  distinct (case when t.xuni_id is not null   or t.org_id is  not null then t.id end) as id,2  as  resource_type ,"${DATE}" as time
                         |from
                         |(
                         |					select c.id id,x.id xuni_id,s.org_id org_id  from neworiental_v3.entity_exercise c
                         |					left join neworiental_user.entity_user u on u.system_id=c.created_by
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=c.created_by
                         |					where c.is_finished_product!=1 and  c.status=1 and s.private=0 and s.enable=1 and u.org_type=2
                         |					and (to_date(c.submit_time) = "${DATE}" or to_date(c.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id  from neworiental_v3.entity_exercise c
                         |					join neworiental_v3.link_custom_list_resource r on r.resource_id=c.id
                         |					left join neworiental_user.entity_user u on u.system_id=r.system_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.system_id
                         |					where c.is_finished_product!=1 and  c.status=1 and r.is_fav=1 and s.private=0 and s.enable=1 and r.resource_type=2  and u.org_type=2
                         |					and (to_date(r.create_time) = "${DATE}" or to_date(r.update_time) = "${DATE}")
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id  from neworiental_v3.entity_exercise c
                         |					join neworiental_v3.link_respackage_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage r on r.id=l.package_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and l.resource_type=2 and u.org_type=2 and s.enable=1
                         |					and to_date(r.create_time) = "${DATE}"
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id  from neworiental_v3.entity_exercise c
                         |					join neworiental_v3.link_respackage_publish_resource l on l.resource_id=c.id
                         |					join neworiental_v3.entity_respackage_publish r on r.id=l.publish_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.status=1 and s.private=0 and l.resource_type=2 and u.org_type=2 and s.enable=1
                         |					and  to_date(r.create_time) = "${DATE}"
                         |)t  """.stripMargin
    sql(str_create_tj)
    /*题目成品*/
    val str_drop_tm=""" drop table test.question_finished_product """.stripMargin
    sql(str_drop_tm)
    val str_create_tm=s"""  create table test.question_finished_product as
                         |select  distinct (case when t.xuni_id is not null   or t.org_id is  not null then t.id end) as id  ,4  as  resource_type ,"${DATE}" as time
                         |
 |from (
                         |					select c.id as id,x.id xuni_id,s.org_id org_id    from neworiental_v3.entity_question c
                         |					left join neworiental_user.entity_user u on u.system_id=c.upload_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=c.upload_id
                         |					where c.is_finished_product!=1 and c.state='ENABLED' and s.private=0 and s.enable=1  and u.org_type=2 and
                         |					(to_date(c.upload_time) = "${DATE}"  or  to_date(c.update_time)  = "${DATE}")
                         |					union all
                         |					select c.id as id,x.id xuni_id,s.org_id org_id   from neworiental_v3.entity_question c
                         |					join neworiental_v3.link_exercise_question l on l.question_id=c.id
                         |					join neworiental_v3.link_custom_list_resource r on r.resource_id=l.exercise_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.system_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.system_id
                         |					where  c.is_finished_product!=1 and c.state='ENABLED' and r.is_fav=1 and s.private=0 and s.enable=1 and r.resource_type=2 and u.org_type=2
                         |					and (to_date(r.create_time)  = "${DATE}" or to_date(r.update_time) = "${DATE}")
                         |					union all
                         |					select c.id as id,x.id xuni_id,s.org_id org_id   from neworiental_v3.entity_question c
                         |					join neworiental_v3.link_exercise_question le on le.question_id=c.id
                         |					join neworiental_v3.link_respackage_resource l on l.resource_id=le.exercise_id
                         |					join neworiental_v3.entity_respackage r on r.id=l.package_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where  c.is_finished_product!=1 and c.state='ENABLED' and s.private=0 and l.resource_type=2 and u.org_type=2 and s.enable=1 and
                         |					to_date(r.create_time)   = "${DATE}"
                         |					union all
                         |					select c.id id,x.id xuni_id,s.org_id org_id   from neworiental_v3.entity_question c
                         |					join neworiental_v3.link_exercise_question le on le.question_id=c.id
                         |					join neworiental_v3.link_respackage_publish_resource l on l.resource_id=le.exercise_id
                         |					join neworiental_v3.entity_respackage_publish r on r.id=l.publish_id
                         |					left join neworiental_user.entity_user u on u.system_id=r.creator_id
                         |					join neworiental_logdata.entity_school s on u.org_id=s.org_id
                         |					left join test.xuli_list x  on x.id=r.creator_id
                         |					where c.is_finished_product!=1 and c.state='ENABLED' and s.private=0 and l.resource_type=2 and u.org_type=2 and s.enable=1
                         |					and  to_date(r.create_time) = "${DATE}"
                         |)t """.stripMargin
    sql(str_create_tm)

    /*成品汇总表*/
    val str_drop_fpr=""" drop table neworiental_report.finished_product_resource  """.stripMargin
    sql(str_drop_fpr)
    val str_create_fpr="""  create table neworiental_report.finished_product_resource as
                         |select  row_number() over() as id ,tmp.id as resource_id,tmp.resource_type,tmp.time from(
                         |select * from test.clip_finished_product
                         |union all
                         |select * from test.courseware_finished_product
                         |union all
                         |select * from test.exercise_finished_product
                         |union all
                         |select * from test.question_finished_product
                         |) tmp  """.stripMargin
    sql(str_create_fpr)
    sc_1.stop()
  }
}
