package H5push
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date

//import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.DefaultHttpClient
//import org.apache.http.impl.client.HttpClientBuilder

import Predef._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
//import org.apache.spark.sql.Row._
//
//import scala.runtime.AbstractFunction1
//import scala.util.matching.Regex
//import scala.util.matching.Regex.MatchIterator
//import scala.collection.immutable.StringOps
//import scala.runtime.BoxedUnit
//import scala.runtime.BoxesRunTime
//import org.apache.spark.sql.types.IntegerType._
//import org.apache.spark.sql.types.LongType._
//import org.apache.spark.sql.types.StringType._
//import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.types.StructField._
//import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types.IntegerType
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response
import redis.clients.jedis._
import java.util.ArrayList
//import javax.servlet.http.HttpServlet
//import javax.servlet.http.HttpServletRequest
//import javax.servlet.http.HttpServletResponse
//import javax.servlet.ServletException
//import java.io.PrintWriter
import java.io.IOException
import java.net.URLEncoder
import scala.collection.mutable.ArrayBuffer


//import org.apache.commons.httpclient.HttpClient
//import org.apache.commons.httpclient.methods.InputStreamRequestEntity
//import org.apache.commons.httpclient.methods.PostMethod
//import org.apache.commons.httpclient.methods.RequestEntity
//import org.apache.http.HttpEntity
//import org.apache.http.HttpResponse
//import org.apache.http.NameValuePair
////import org.apache.http.client.HttpClient
//import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
//import org.apache.http.message.BasicNameValuePair
//import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
//import java.util.Map
//import java.util
//import java.util.Map.Entry
//import org.apache.http.client.methods.CloseableHttpResponse
//import org.apache.http.impl.client.CloseableHttpClient
//import org.apache.http.impl.client.HttpClients
import org.apache.http.client.ClientProtocolException

import org.apache.http.entity.StringEntity
import org.json.{JSONArray, JSONObject}

//import java.net.URLEncoder
//import java.net.URLDecoder

import java.sql.{DriverManager, PreparedStatement, Connection}

import java.util.Properties
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.types.DataTypes

import scala.Serializable


object h5push {
  def main(args: Array[String]): Unit = {

    val cnx = new SparkConf().setAppName("spark_h5_1").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(cnx)

    //      val path = "/user/kafka/praxis-svr-test/" + args(0) + "/" + args(1) + "/"
    //      val fileRdd = sc.textFile(path, 2)
    //      val rrr = fileRdd.filter( line => {line.matches(".*req: set.*r.*")})
    //      //fileRdd.saveAsTextFile("/user/hue/wej/fileRdd")
    //      //fileRdd.toString().matches(".*req: set.*r.*")
    //      //val fileRdd = "imestamp1506648599436agentHost127.0.0.filed/xdfapp/logs/praxis-svr/server-ssdb.log.2017-09-29headerKey1log¨2017-09-29 09:29:59 INFO com.noriental.praxissvr.answer.util.PraxisSsdbUtil:45 [DubboServerHandler-172.18.4.145:24001-thread-188] [020582403511]  req: set se5_12990314_81951147304_7295004r 1,resp: ok 5 ,1_3_14458" +"timestamp1506648599436agentHost127.0.0.filed/xdfapp/logs/praxis-svr/server-ssdb.log.2017-09-29headerKey1log¨2017-09-29 09:29:59 INFO com.noriental.praxissvr.answer.util.PraxisSsdbUtil:45 [DubboServerHandler-172.18.4.145:24001-thread-188] [020582403511]  req: set se5_12990314_81951147304_6234201r 1,resp: ok 5 ,1_3_14458" +"timestamp1506648599436agentHost127.0.0.filed/xdfapp/logs/praxis-svr/server-ssdb.log.2017-09-29headerKey1log¨2017-09-29 09:29:59 INFO com.noriental.praxissvr.answer.util.PraxisSsdbUtil:45 [DubboServerHandler-172.18.4.145:24001-thread-188] [020582403511]  req: set se5_12990314_81951147304_5912699r 1,resp: ok 5 ,1_3_14458"
    //      val re1 = """.*([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2})(:[0-9]{2}:[0-9]{2}).*req: set se([0-9]+?)_([0-9]+?)_([0-9]+?)_([0-9]+)(.*?) (.*),resp: ok.*""".r
    //      //val reg = """.*([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2})(:[0-9]{2}:[0-9]{2}).*req: set se([0-9]+?)_([0-9]+?)_([0-9]+?)_([0-9]+)(.*?) (.*),resp: ok.*""".r
    //      val rowrdd = rrr.map(line => {
    //        val rdd = re1.findAllIn(line.toString())
    //        rdd.hasNext
    //        var time: String = "0000-00-00 00:00:00"
    //        var exercise_source: java.lang.Integer = 0
    //        var resource_id: java.lang.Long = 0L
    //        var student_id: java.lang.Long = 0L
    //        var question_id: java.lang.Long = 0L
    //        var ctype: String = ""
    //        var value: String = ""
    //        var day: java.lang.Integer = 999999
    //        var h: String = "00"
    //        if (line.matches(".*req: set.*r.*")) {
    //          if (rdd.groupCount == 9) {
    //            time = new StringBuilder().append(rdd.group(1)).append(" ").append(rdd.group(2)).append(rdd.group(3)).toString()
    //            //println(time)
    //            exercise_source = rdd.group(4).toInt
    //            resource_id = rdd.group(5).toLong
    //            student_id = rdd.group(6).toLong
    //            question_id = rdd.group(7).toLong
    //            ctype = rdd.group(8)
    //            value = rdd.group(9)
    //            day = rdd.group(1).replaceAll("-", "").toInt
    //            h = rdd.group(2)
    //          } else {
    //            Predef.println(new StringBuilder().append("### log format err ###").append(rdd).toString())
    //          }
    //        }
    //        val row = RowFactory.create(time, exercise_source, resource_id, student_id, question_id, ctype, value, day, h)
    //        row
    //      })
    //
    //      val structFields = new ArrayList[StructField]()
    //      structFields.add(DataTypes.createStructField("time", DataTypes.StringType, true))
    //      structFields.add(DataTypes.createStructField("exercise_source", DataTypes.IntegerType, true))
    //      structFields.add(DataTypes.createStructField("resource_id", DataTypes.LongType, true))
    //      structFields.add(DataTypes.createStructField("student_id", DataTypes.LongType, true))
    //      structFields.add(DataTypes.createStructField("question_id", DataTypes.LongType, true))
    //      structFields.add(DataTypes.createStructField("ctype", DataTypes.StringType, true))
    //      structFields.add(DataTypes.createStructField("value", DataTypes.StringType, true))
    //      structFields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true))
    //      structFields.add(DataTypes.createStructField("h", DataTypes.StringType, true))
    //      val structType = DataTypes.createStructType(structFields)


    //      val hiveCtx = new HiveContext(sc)
    //      hiveCtx.setConf("hive.exec.dynamic.partition", "true")
    //      hiveCtx.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    //      hiveCtx.setConf("hive.exec.max.dynamic.partitions.pernode", "10000")
    //      hiveCtx.setConf("spark.sql.shuffle.partitions", "1")
    //
    //      hiveCtx.sql("use h5")

    //      val Df = hiveCtx.createDataFrame(rowrdd, structType)
    //      Df.registerTempTable("persons")
    //      hiveCtx.sql("insert overwrite table h5.push_test PARTITION (day,h) select time, exercise_source, resource_id, student_id, question_id, ctype, value, day, h from persons")
    //
    //      //插入历史答题
    //      val sql1 = "insert into h5.student_his_que partition(day='"+args(0)+"') select distinct student_id, question_id from h5.push_test where day = '"+args(0)+"' and h = '"+args(1)+"'"
    //
    //      hiveCtx.sql(sql1)

    if (args(1).toInt >= 7) {
      //      //符合历史答题数大于15/20
      //      val sql2 = "insert overwrite table h5.student_his_que_15 select distinct pd.student_id, pd.topic_id, pd.cnt, pd.h2type from ( select  hz.student_id,  hz.topic_id,  hz.cnt,   hz.h2type,  case when wp.push_time is null then 1 else 0 end as weektype from (  select  hq.student_id,  lkqt.topic_id,  count(distinct hq.question_id) cnt,  case when count(distinct hq.question_id) >= 20 then 1 else 0 end as h2type  from h5.student_his_que hq  inner join neworiental_v3.link_question_topic lkqt  on lkqt.question_id = hq.question_id  inner join neworiental_v3.entity_question que  on que.id = hq.question_id  where que.is_single = 1  group by hq.student_id,  lkqt.topic_id  having cnt >= 15 ) hz left join ( select distinct student_id, topic_id, push_time  from h5.entity_h5_push_record where push_time >= date_sub(push_time, 14)  ) wp on wp.student_id = hz.student_id and wp.topic_id = hz.topic_id ) pd where pd.weektype = 1"
      //      hiveCtx.sql(sql2)
      //
      //      //取h1人口
      //
      //      val sql3 = "select distinct student_id, topic_id from h5.student_his_que_15 hq"
      //      val h1r = hiveCtx.sql(sql3)
      //
      //      val properties = new Properties()
      //      properties.setProperty("user", "admin")
      //      properties.setProperty("password", "qq9527@xdf")
      //
      //      //取15正确率
      //      if (h1r.count() > 0) {
      //        val dataf = redis_topic(h1r, args(0).toString)
      //
      //        val structFields2 = new ArrayList[StructField]()
      //        structFields2.add(DataTypes.createStructField("student_id", DataTypes.StringType, true))
      //        structFields2.add(DataTypes.createStructField("topic", DataTypes.StringType, true))
      //        structFields2.add(DataTypes.createStructField("rate", DataTypes.StringType, true))
      //        structFields2.add(DataTypes.createStructField("day", DataTypes.StringType, true))
      //        val structType2 = DataTypes.createStructType(structFields2)
      //
      //        val Df2 = hiveCtx.createDataFrame(dataf, structType2)
      //        Df2.registerTempTable("push_done_h1")
      //        //插入h1
      //        hiveCtx.sql("insert overwrite table h5.h1_will_push select distinct student_id, topic, rate, day  from push_done_h1")
      //        //与平均正确率判断
      //        val sql4 = "insert overwrite table h5.h1_push select wp.student_id, wp.topic_id, wp.rate, jtr.right_ratio, us.org_id, us.org_type, pp.system_id parent_id, top.name topic_name, '1', top.subject_id, from_unixtime(unix_timestamp(wp.day, 'yyyyMMdd'), 'yyyy-MM-dd') create_time from h5.h1_will_push wp inner join neworiental_report.jiazhang_topic_ratio jtr on wp.topic_id = jtr.topic_id inner join neworiental_user.entity_user us on us.system_id = wp.student_id inner join neworiental_user.entity_profile_parent pp on pp.student_system_id = wp.student_id inner join neworiental_v3.entity_topic top on top.id = wp.topic_id where (wp.rate - jtr.right_ratio) >= 0.1 and wp.rate >= 0.8"
      //        hiveCtx.sql(sql4)
      //        val sql4_1 = "select * from h5.h1_push"
      //        val fs = hiveCtx.sql(sql4_1)
      //
      //        var i = 0
      //        val url = "http://10.60.0.62:2022/recommend/createReportBatch"
      //        val charset = "utf-8"
      //
      //        var map_h1 = Map[Int, JSONObject]()
      //        //val nums: Map[Int, JSONObject] = Map()
      //        for (dt <- fs) {
      //          val student_id = dt.get(0)
      //          val topic_id = dt.get(1)
      //          val rate = dt.get(2)
      //          val avg_rate = dt.get(3)
      //          val org_id = dt.get(4)
      //          val org_type = dt.get(5)
      //          val parent_id = dt.get(6)
      //          val topic_name = dt.get(7)
      //          val templateId = dt.get(8)
      //          val subject_id = dt.get(9)
      //          val json = "{\"data\":\"{\\\"topic_id\\\":"+topic_id+",\\\"right_rate\\\":"+rate+",\\\"avg_rate\\\":"+avg_rate+",\\\"topicName\\\":\\\""+topic_name+"\\\",\\\"subjectId\\\":"+subject_id+"}\",\"orgId\":"+org_id+",\"orgType\":"+org_type+",\"outLinkTitle\":\"您的孩子"+topic_name+"知识点学的太棒啦，快来看看吧\",\"parentId\":"+parent_id+",\"studentId\":"+student_id+",\"templateId\":1}"
      //          val js = new JSONObject(json)
      //
      //          if (i <= 5000) {
      //            map_h1 ++= Map(i -> js)
      //          } else {
      //            doPost(url, map_h1, charset)
      //            map_h1 = Map[Int, JSONObject]()
      //            i = 0
      //          }
      //
      //          i = i + 1
      //
      //
      //          //        post方式调用http接口的步骤：
      //          //        * 1.构造HttpClient实例
      //          //        * 2.构造PostMethod实例
      //          //        * 3.把参数值放入到PostMethod对象中
      //          //        *   方式1:利用NameValuePair类
      //          //        *   方式2:直接用PostMethod实例对象的addParameter方法
      //          //        * 4.执行postMethod,调用http接口
      //          //        * 5.读取内容
      //          //        * 6.处理返回的内容
      //          //        * 7.释放连接
      ////          admintest    dsjw2015
      ////
      ////          admin       qq9527@xdf
      ////
      ////          reader                1a2s3dqwe
      //        }
      //
      //      }
      //
      //      val h1_app = hiveCtx.sql("select hp.student_id, hp.parent_id, hp.topic_id, 1 type, hp.rate before_right_rate, hp.rate after_right_rate, hp.right_ratio avg_right_rate, hq15.cnt question_count, hp.create_time push_time from h5.h1_push hp inner join h5.student_his_que_15 hq15 on hq15.student_id = hp.student_id and hq15.topic_id = hp.topic_id")
      //
      //      //线上
      //      h1_app.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/test", "entity_h5_push_record", properties)
      //      hiveCtx.sql("insert into h5.entity_h5_push_record select hp.student_id, hp.parent_id, hp.topic_id, 1 type, hp.rate before_right_rate, hp.rate after_right_rate, hp.right_ratio avg_right_rate, hq15.cnt question_count, hp.create_time push_time from h5.h1_push hp inner join h5.student_his_que_15 hq15 on hq15.student_id = hp.student_id and hq15.topic_id = hp.topic_id")
      //
      //
      //      h2_push(hiveCtx)
      //      val h2_app = hiveCtx.sql("select hp.student_id, hp.parent_id, hp.topic_id, 2 type, hp.rate before_right_rate, hp.rate after_right_rate, hp.right_ratio avg_right_rate, hq15.cnt question_count, hp.create_time push_time from h5.h2_push hp inner join h5.student_his_que_15 hq15 on hq15.student_id = hp.student_id and hq15.topic_id = hp.topic_id")
      //      //线上
      //       h2_app.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/test", "entity_h5_push_record", properties)
      //      hiveCtx.sql("insert into h5.entity_h5_push_record select hp.student_id, hp.parent_id, hp.topic_id, 1 type, hp.rate before_right_rate, hp.rate after_right_rate, hp.right_ratio avg_right_rate, hq15.cnt question_count, hp.create_time push_time from h5.h2_push hp inner join h5.student_his_que_15 hq15 on hq15.student_id = hp.student_id and hq15.topic_id = hp.topic_id")

      //h3-4

      //      val sql3_4 = "insert into test.h5_help_record select hz.student_id, hz.topic_id, round(hz.before_rate, 2), round(hz.after_rate, 2), us.org_id, us.org_type, pp.system_id parent_id, top.name topic_name, (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase, top.subject_id, hz.day from ( select distinct tl.student_id,  tl.topic_id topic_id,  bf.right_rate before_rate,  tl.right_rate after_rate,  substr(tl.create_time,1,10) day from neworiental_v3.entity_trail_lesson tl inner join (  select  *  from neworiental_v3.entity_trail_lesson tl  where  tl.op_type = 1  and tl.topic_type = 1  and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10)  ) bf on bf.student_id = tl.student_id  and bf.teacher_id = tl.teacher_id  and bf.topic_id = tl.topic_id  and  bf.publish_id = tl.publish_id  and substr(bf.create_time,1,10) = substr(tl.create_time,1,10) where tl.op_type = 2  and tl.topic_type = 1  and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')  and tl.create_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour) ) hz inner join neworiental_user.entity_user us on us.system_id = hz.student_id inner join neworiental_user.entity_profile_parent pp on pp.student_system_id = hz.student_id inner join neworiental_v3.entity_topic top on top.id = hz.topic_id"
      //      val t_sql3_4 = "truncate table test.h5_help_record"
      //
      //      var connection : Connection = null
      //      try {
      //        //4.2
      //        connection = DriverManager.getConnection("jdbc:mysql://10.10.6.7:3306/test?useSSL=false", "admintest", "dsjw2015")
      //        val ps : PreparedStatement = connection.prepareStatement(t_sql3_4)
      //        ps.execute()
      //        ps.close()
      //        val ps2 : PreparedStatement = connection.prepareStatement(sql3_4)
      //        ps2.execute()
      //        ps2.close()
      //      } catch {
      //        case e => println("comm err "+e)
      //      }
      //      connection.close()
      //4.2
      val sqlContext = new SQLContext(sc)
      val h3df = sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://172.18.4.3:3306/neworiental_v3",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "(select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time,  org_id,  org_type,  subject_id,  topic_name from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type   ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase < 0) as h3",
        "user" -> "admin","password" -> "qq9527@xdf")).load()

      val h3df_2 = sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://172.18.4.3:3306/neworiental_v3",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "(select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time,  org_id,  org_type,  subject_id,  topic_name from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type   ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase < 0) as h3",
        "user" -> "admin","password" -> "qq9527@xdf")).load()

      val sqlh3_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time,topic_type) select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time, 1 topic_type from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase < 0"
      val sqlh3_2_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time,topic_type) select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time, 2 topic_type from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type  ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase < 0"


      h3_push(h3df, sqlh3_mysql)
      h3_push(h3df_2, sqlh3_2_mysql)
      //val h3_app = hiveCtx.sql("select student_id, parent_id, topic_id, 3 type, before_rate before_right_rate, after_rate after_right_rate, 0 avg_right_rate, 0 question_count, datetime push_time from h5.h5_help_record hhr where hhr.increase > 0")
      //h3_app.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/h5", "entity_h5_push_record", properties)
      //hiveCtx.sql("insert into h5.entity_h5_push_record select student_id, parent_id, topic_id, 3 type, before_rate before_right_rate, after_rate after_right_rate, 0 avg_right_rate, 0 question_count, datetime push_time from test.h5_help_record hhr where hhr.increase > 0")
      //4.2
      val h4df = sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://172.18.4.3:3306/neworiental_v3",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "(select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time,  org_id,  org_type,  subject_id,  topic_name from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type   ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase >= 0) as h4",
        "user" -> "admin","password" -> "qq9527@xdf")).load()

      val h4df_2 = sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://172.18.4.3:3306/neworiental_v3",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "(select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time,  org_id,  org_type,  subject_id,  topic_name from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type   ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase >= 0) as h4",
        "user" -> "admin","password" -> "qq9527@xdf")).load()

      val sqlh4_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time,topic_type) select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time, 1 topic_type from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase >= 0"
      val sqlh4_2_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time,topic_type) select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time, 2 topic_type from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10)  and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')   group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type  ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase >= 0"


      h4_push(h4df, sqlh4_mysql)
      h4_push(h4df_2, sqlh4_2_mysql)
      //val h4_app = hiveCtx.sql("select student_id, parent_id, topic_id, 4 type, before_rate before_right_rate, after_rate after_right_rate, 0 avg_right_rate, 0 question_count, datetime push_time from h5.h5_help_record hhr where hhr.increase <= 0")
      //h4_app.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/h5", "entity_h5_push_record", properties)
      //hiveCtx.sql("insert into h5.entity_h5_push_record select student_id, parent_id, topic_id, 4 type, before_rate before_right_rate, after_rate after_right_rate, 0 avg_right_rate, 0 question_count, datetime push_time from test.h5_help_record hhr where hhr.increase <= 0")

      //      val sqlh3_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time) select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase < 0"
      //      val sqlh4_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time) select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   top.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   top.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 1   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.topic_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 1   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_topic top  on top.id = hz.topic_id ) hhr where hhr.increase >= 0"
      //      val sqlh3_2_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time) select  distinct  student_id,  parent_id,  topic_id,  3 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type  ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase < 0"
      //      val sqlh4_2_mysql = "insert into neworiental_v3.entity_h5_push_record (student_id,parent_id,topic_id,type,before_right_rate,after_right_rate,avg_right_rate,question_count,push_time) select  distinct  student_id,  parent_id,  topic_id,  4 type,  before_rate before_right_rate,  after_rate after_right_rate,  0 avg_right_rate,  0 question_count,  day push_time from (  select   hz.student_id,   hz.topic_id,   round(hz.before_rate, 2) before_rate,   round(hz.after_rate, 2) after_rate,   us.org_id,   us.org_type,   pp.system_id parent_id,   chp.name topic_name,   (round(hz.before_rate, 2) - round(hz.after_rate, 2)) increase,   chp.subject_id,   hz.day  from (   select    DISTINCT    ft.student_id,    ft.topic_id,    case when bf.right_rate is null then 0 else bf.right_rate end before_rate,    case when ft.right_rate is null then 0 else ft.right_rate end after_rate,    ft.create_time day   from (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      min(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 1      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10)  and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')   group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) bf    on bf.publish_id = tl.publish_id     and bf.student_id = tl.student_id     and bf.teacher_id = tl.teacher_id     and bf.topic_id = tl.topic_id     and bf.topic_type = tl.topic_type     and bf.create_time = tl.create_time    where tl.topic_type = 2   ) bf   inner join (    select     DISTINCT tl.*    from neworiental_v3.entity_trail_lesson tl    inner join (     select      student_id,      teacher_id,      publish_id,      topic_id,      topic_type,      max(create_time) create_time     from neworiental_v3.entity_trail_lesson tl     where tl.op_type = 2      and substr(tl.create_time,1,10) = SUBSTR(NOW(), 1, 10) and tl.create_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    group by student_id,      teacher_id,      publish_id,      topic_id,      topic_type ) ft    on ft.publish_id = tl.publish_id     and ft.student_id = tl.student_id     and ft.teacher_id = tl.teacher_id     and ft.topic_id = tl.topic_id     and ft.topic_type = tl.topic_type     and ft.create_time = tl.create_time    inner join neworiental_v3.entity_help_record hr    on hr.publish_id = tl.publish_id and hr.student_id = tl.student_id and hr.chapter_id = tl.topic_id and hr.teacher_id = tl.teacher_id    where hr.status = 2    and hr.update_time < DATE_FORMAT(SUBSTR(NOW(), 1, 13), '%Y-%m-%d %H:%i:%s')    and hr.update_time >= DATE_SUB(SUBSTR(NOW(), 1, 13), interval 1 hour)    and tl.topic_type = 2   ) ft   on bf.publish_id = ft.publish_id     and bf.student_id = ft.student_id     and bf.teacher_id = ft.teacher_id     and bf.topic_id = ft.topic_id     and bf.topic_type = ft.topic_type  ) hz  inner join neworiental_user.entity_user us  on us.system_id = hz.student_id  inner join neworiental_user.entity_profile_parent pp  on pp.student_system_id = hz.student_id  inner join neworiental_v3.entity_teaching_chapter chp  on chp.id = hz.topic_id ) hhr where hhr.increase >= 0"
      //
      //      var connection2 : Connection = null
      //      try {
      //        //线上mysql
      //        connection2 = DriverManager.getConnection("jdbc:mysql://10.10.6.7:3307/neworiental_v3?useSSL=false", "admintest", "dsjw2015")
      //        val ps : PreparedStatement = connection2.prepareStatement(sqlh3_mysql)
      //        ps.execute()
      //        ps.close()
      //        val ps2 : PreparedStatement = connection2.prepareStatement(sqlh4_mysql)
      //        ps2.execute()
      //        ps2.close()
      //        val ps3 : PreparedStatement = connection2.prepareStatement(sqlh3_2_mysql)
      //        ps3.execute()
      //        ps3.close()
      //        val ps4 : PreparedStatement = connection2.prepareStatement(sqlh4_2_mysql)
      //        ps4.execute()
      //        ps4.close()
      //      } catch {
      //        case e => println("comm err "+e)
      //      }
      //      connection2.close()

      //hiveCtx.sql()

    }

    //    def h1_push(hiveCtx:HiveContext) = {
    //
    //    }
    //
    //    def h2_push(hiveCtx:HiveContext)  = {
    //      val sql3 = "select distinct student_id,topic_id from h5.student_his_que_15 hq where hq.h2type = 1"
    //      val h1r = hiveCtx.sql(sql3)
    //      //取15正确率
    //      if (h1r.count() > 0) {
    //        val dataf = redis_topic(h1r, args(0))
    //
    //        val structFields2 = new ArrayList[StructField]()
    //        structFields2.add(DataTypes.createStructField("student_id", DataTypes.StringType, true))
    //        structFields2.add(DataTypes.createStructField("topic", DataTypes.StringType, true))
    //        structFields2.add(DataTypes.createStructField("rate", DataTypes.StringType, true))
    //        structFields2.add(DataTypes.createStructField("day", DataTypes.StringType, true))
    //        val structType2 = DataTypes.createStructType(structFields2)
    //
    //        val Df2 = hiveCtx.createDataFrame(dataf, structType2)
    //        Df2.registerTempTable("push_done_h2")
    //        //插入h2
    //        hiveCtx.sql("insert overwrite table h5.h2_will_push select student_id, topic, rate, day from push_done_h2")
    //        //与平均正确率判断
    //        val sql4 = "insert overwrite table h5.h2_push select wp.student_id, wp.topic_id, wp.rate, jtr.right_ratio, us.org_id, us.org_type, pp.system_id parent_id, top.name topic_name, '1', top.subject_id, from_unixtime(unix_timestamp(wp.day, 'yyyyMMdd'), 'yyyy-MM-dd') create_time from h5.h1_will_push wp inner join neworiental_report.jiazhang_topic_ratio jtr on wp.topic_id = jtr.topic_id inner join neworiental_user.entity_user us on us.system_id = wp.student_id inner join neworiental_user.entity_profile_parent pp on pp.student_system_id = wp.student_id inner join neworiental_v3.entity_topic top on top.id = wp.topic_id inner join neworiental_user.l_class_student lcs lcs.student_system_id = wp.student_id inner join neworiental_v3.entity_help_count hc on hc.student_id = wp.student_id where wp.rate < 0.7 and hc.status = 1"
    //        val sql4_2 = "select * from h5.h2_push"
    //        val fs = hiveCtx.sql(sql4_2)
    //
    //        var i = 0
    //        val url = "http://10.60.0.62:2022/recommend/createReportBatch"
    //        val charset = "utf-8"
    //
    //        var map = Map[Int, JSONObject]()
    //        for (dt <- fs) {
    //          val student_id = dt.get(0)
    //          val topic_id = dt.get(1)
    //          val rate = dt.get(2)
    //          val avg_rate = dt.get(3)
    //          val org_id = dt.get(4)
    //          val org_type = dt.get(5)
    //          val parent_id = dt.get(6)
    //          val topic_name = dt.get(7)
    //          val templateId = dt.get(8)
    //          val subject_id = dt.get(9)
    //          val json = "{\"data\":\"{\\\"topic_id\\\":"+topic_id+",\\\"right_rate\\\":"+rate+",\\\"avg_rate\\\":"+avg_rate+",\\\"topicName\\\":\\\""+topic_name+"\\\",\\\"subjectId\\\":"+subject_id+"}\",\"orgId\":"+org_id+",\"orgType\":"+org_type+",\"outLinkTitle\":\"您的孩子"+topic_name+"知识点仍未掌握，快来看看吧\",\"parentId\":"+parent_id+",\"studentId\":"+student_id+",\"templateId\":1}"
    //          val js = new JSONObject(json)
    //
    //          if (i <= 5000) {
    //            map ++= Map(i -> js)
    //          } else {
    //            doPost(url, map, charset)
    //            map = Map[Int, JSONObject]()
    //            i = 0
    //          }
    //          i = i + 1
    //        }
    //      }
    //
    //
    //    }

    def h3_push(h3df:DataFrame, sql:String) = {
      val h3_data = h3df.collect()
      var i = 0
      val url = "http://adapter.okjiaoyu.cn/recommend/createReportBatch"
      val charset = "utf-8"
      var map = Map[Int, JSONObject]()
      for (dt <- h3_data) {
        val student_id = dt.get(0)
        val parent_id = dt.get(1)
        val topic_id = dt.get(2)
        val type_id = dt.get(3)
        val before_rate = dt.get(4)
        val after_rate = dt.get(5)
        val org_id = dt.get(9)
        val org_type = dt.get(10)
        val topic_name = dt.get(12)
        val cut = dt.get(7)
        val subject_id = dt.get(11)
        val json = "{\"data\":\"{\\\"topic_id\\\":"+topic_id+",\\\"before_rate\\\":"+before_rate+",\\\"after_rate\\\":"+after_rate+",\\\"topicName\\\":\\\""+topic_name+"\\\",\\\"subjectId\\\":"+subject_id+"}\",\"orgId\":"+org_id+",\"orgType\":"+org_type+",\"outLinkTitle\":\"您的孩子上完辅导后"+topic_name+"知识点取得了进步，快来看看吧\",\"parentId\":"+parent_id+",\"studentId\":"+student_id+",\"templateId\":3}"
        val js = new JSONObject(json)
        println("----------------------------------------------------------------------------")
        println(json)
        if (i <= 5000) {
          map ++= Map(i -> js)
        } else {
          //doPost(url, map, charset)
          doPost_str(url, map, charset, sql)
          map = Map[Int, JSONObject]()
          i = 0
          map ++= Map(i -> js)
        }
        i = i + 1
      }
      if (h3_data.length > 0) {
        //doPost(url, map, charset)
        doPost_str(url, map, charset, sql)
      }

    }

    def h4_push(h4df:DataFrame, sql:String) = {
      println("uuuuuuuuuuuuuuuuu     h4_push")
      val h4_data = h4df.collect()
      var i = 0
      val url = "http://adapter.okjiaoyu.cn/recommend/createReportBatch"
      val charset = "utf-8"

      var map = Map[Int, JSONObject]()
      for (dt <- h4_data) {
        val student_id = dt.get(0)
        val parent_id = dt.get(1)
        val topic_id = dt.get(2)
        val type_id = dt.get(3)
        val before_rate = dt.get(4)
        val after_rate = dt.get(5)
        val org_id = dt.get(9)
        val org_type = dt.get(10)
        val topic_name = dt.get(12)
        val cut = dt.get(7)
        val subject_id = dt.get(11)
        val json = "{\"data\":\"{\\\"topic_id\\\":"+topic_id+",\\\"before_rate\\\":"+before_rate+",\\\"topicName\\\":\\\""+topic_name+"\\\",\\\"subjectId\\\":"+subject_id+"}\",\"orgId\":"+org_id+",\"orgType\":"+org_type+",\"outLinkTitle\":\"您的孩子经过辅导后"+topic_name+"知识点没有提升，我们会持续辅导孩子，快来看看吧\",\"parentId\":"+parent_id+",\"studentId\":"+student_id+",\"templateId\":4}"
        val js = new JSONObject(json)
        println("-------------------------------------------------------------------")
        println(json)
        if (i <= 5000) {
          map ++= Map(i -> js)
          println(map.get(i))
        } else {
          //doPost(url, map, charset)
          doPost_str(url, map, charset, sql)
          map = Map[Int, JSONObject]()
          i = 0
          map ++= Map(i -> js)
          println(map.get(i))
        }
        i = i + 1
      }
      if (h4_data.length > 0) {
        println("h4_data.count()---------------" + h4_data.length)
        //doPost(url, map, charset)
        doPost_str(url, map, charset, sql)
      }
    }

    //hive->mysql
    //    val hiveCtx = new HiveContext(sc)
    //    val h1_app = hiveCtx.sql("select * from test.canal_test_table_app")
    //    val properties = new Properties()
    //    properties.setProperty("user", "admin")
    //    properties.setProperty("password", "qq9527@xdf")
    //    h1_app.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/test", "canal_test_table_app", properties)



    //    hiveCtx.sql("insert into test.")
    //
    //    val redisHost = "10.60.0.125"
    //    val redisPort = 29100
    //val student_id =

    //    val redisClient = new Jedis(redisHost, redisPort)
    //    var topic_master_t = Map[String, Response[String]]()
    //    redisClient.select(1)
    //    val pp_t = redisClient.pipelined()
    //    pp_t.sync()
    //    var topic_master = Map[String,Double]()

    //    val redisHost = "10.10.6.7"
    //    val redisPort = 6379
    //    val student_id = "81951085079"
    //    val topic = "15728"
    //
    //    var i = 0
    //    val redisClient = new Jedis(redisHost, redisPort)
    //    // redisClient.select(1)
    //    ////知识点
    //    val pp_t = redisClient.pipelined()
    //
    //    val key_t = "accuracy_1_" + student_id + "_" + topic
    //    val key_t2 = "accuracy_1_81104100_3086"
    //    val dd = pp_t.get(key_t)
    //    println(dd)
    //    pp_t.sync()
    //    println(key_t)
    //    println(dd.get())
    //    if (dd.get() != null) {
    //      val ss = Math.round((dd.get().toDouble) * 100) / 100.0
    //      println(ss)
    //    }
    //    redisClient.close()


    def redis_topic (data:DataFrame, d:String) : RDD[Row] = {
      val redisHost = "10.10.6.7"
      val redisPort = 6379
      val day = d
      //      val student_id = sd
      //      val topic = tp

      var map = Map[String,Response[String]]()
      var i = 0
      val redisClient = new Jedis(redisHost, redisPort)
      // redisClient.select(1)
      val ppl = redisClient.pipelined()
      //var rate1 = "0.01"
      //data.map(line => "accuracy_1_" + line.get(0).toString + "_" + line.get(1).toString).saveAsTextFile("/user/hue/wej/datamap")
      val key_arry = data.map(line => "accuracy_1_" + line.get(0).toString + "_" + line.get(1).toString).collect()
      for (key <- key_arry) {
        map ++= Map(key -> ppl.get(key))
      }
      ppl.sync()
      //      var topic_map = Map[String,String]()
      //      for (key <- map.keys) {
      //        topic_map ++= Map(key -> map(key).get())
      //      }
      var map_topic = Map[String, Double]()
      for (key <- key_arry) {
        var rate1 = 0.01
        if (map(key).get() != null) {
          rate1 = Math.round(map(key).get().toDouble * 100) / 100.0

        }
        map_topic ++= Map(key -> rate1)
      }
      val dataf = data.map(line => {
        val student_id = line.get(0).toString
        val topic = line.get(1).toString

        val key = "accuracy_1_" + student_id + "_" + topic
        //        val rate = ppl.get(key)
        //        ppl.sync()
        //        val rate1 = rate.get().toString
        val rate1 = map_topic(key)
        val row = RowFactory.create(student_id.toString, topic.toString, rate1.toString, day.toString)
        row
      })
      //ppl.sync()
      redisClient.close()
      return dataf
    }

    //////临时推送
    //    val url = "http://10.60.0.62:2022/recommend/createReportBatch"
    //    val charset = "utf-8"
    //    var map = Map[Int, JSONObject]()
    //    val jin = "{\\\"topic_id\\\":2994,\\\"right_rate\\\":0.86,\\\"avg_rate\\\":0.56,\\\"topicName\\\":\\\"物质跨膜运输的实例\\\",\\\"subjectId\\\":12}"
    //    val jin2 = "{\\\"topic_id\\\":3086,\\\"right_rate\\\":0.66,\\\"avg_rate\\\":0.54,\\\"topicName\\\":\\\"遗传信息的翻译\\\",\\\"subjectId\\\":12}"
    //    val jin3 = "{\\\"topic_id\\\":2324,\\\"before_rate\\\":0.5,\\\"after_rate\\\":0.7,\\\"topicName\\\":\\\"光的色散 颜色\\\",\\\"subjectId\\\":12}"
    //    val jin4 = "{\\\"topic_id\\\":2325,\\\"before_rate\\\":0.5,\\\"topicName\\\":\\\"看不见的光\\\",\\\"subjectId\\\":12}"
    //    val t1 =  "您的孩子物质跨膜运输的实例知识点学的太棒啦，快来看看吧"
    //    val tt = java.net.URLEncoder.encode(t1, "UTF-8")
    //    println(tt)
    //    val ttt = java.net.URLDecoder.decode(tt, "UTF-8")
    //
    ////    val jins = new JSONObject(jin)
    ////    println(jins3)
    ////    val jins2 = new JSONObject(jin2)
    //    //val json = "{\"data\":\""+jin+"\",\"orgId\":80,\"orgType\":2,\"outLinkTitle\":\""+ttt+"\",\"parentId\":99951059996,\"studentId\":81110318,\"templateId\":1}"
    //    //val json2 = "{\"data\":\""+jin2+"\",\"orgId\":80,\"orgType\":2,\"outLinkTitle\":\"您的孩子遗传信息的翻译知识点仍未掌握，快来看看吧\",\"parentId\":99951059996,\"studentId\":81110318,\"templateId\":2}"
    //    val json3 = "{\"data\":\""+jin3+"\",\"orgId\":80,\"orgType\":2,\"outLinkTitle\":\"您的孩子上完智慧辅导后光的色散 颜色知识点取得了进步，快来看看吧\",\"parentId\":99951059996,\"studentId\":81110318,\"templateId\":3}"
    //    val json4 = "{\"data\":\""+jin4+"\",\"orgId\":80,\"orgType\":2,\"outLinkTitle\":\"您的孩子上完智慧辅导后看不见的光知识点没有提升，我们会持续辅导孩子，快来看看吧\",\"parentId\":99951059996,\"studentId\":81110318,\"templateId\":4}"
    //
    //    println(json3)
    //    //val js = new JSONObject(json)
    //    //val js2 = new JSONObject(json2)
    //    val js3 = new JSONObject(json3)
    //    val js4 = new JSONObject(json4)
    //    //map ++= Map(1 -> js)
    //    //map ++= Map(2 -> js2)
    //    map ++= Map(3 -> js3)
    //    map ++= Map(4 -> js4)
    //
    //    doPost(url, map, charset)


    def doPost (url:String,map:Map[Int, JSONObject],charset:String) = {
      //var httpClient:org.apache.http.impl.client.CloseableHttpClient = null
      var httpPost:HttpPost = null
      var result:String = null
      val st = System.currentTimeMillis()
      //val config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build()

      //++
      val httpClient = new DefaultHttpClient()
      val str = st.toString
      try{
        //httpClient = HttpClients.createDefault()
        httpPost = new HttpPost(url)
        httpPost.addHeader("Content-Type","application/json;charset=UTF-8")
        httpPost.addHeader("requestid",str)
        //import java.util
        val list = ArrayBuffer[JSONObject]()
        val list2 = new ArrayList[JSONObject]()

        val iterator = map.iterator
        while (iterator.hasNext) {
          val elem = iterator.next()
          //list.append(elem._2)
          list2.add(elem._2)
        }
        //println(list.toString())
        println("requestid" + st)
        println(list2.toString())

        if(list2.size > 0){
          //val entity = new StringEntity("{\"data\":"+list.toString+"}")
          val entity = new StringEntity("{\"data\":"+list2.toString+"}", "UTF-8")
          entity.setContentEncoding("UTF-8")
          entity.setContentType("application/json")
          httpPost.setEntity(entity)
        }
        val response = httpClient.execute(httpPost)
        val httpEntity = response.getEntity
        println(EntityUtils.toString(httpEntity, "UTF-8"))
        httpPost.releaseConnection()
        //httpClient.close()
        httpClient.getConnectionManager.shutdown()
      }catch{
        case e: Exception => println("exception caught: " + e)
        case c: ClientProtocolException => println("ClientProtocolException caught: " + c)
      }finally {
        try {
          if (httpPost != null) {
            httpPost.releaseConnection()
          }
          if (httpClient != null) {
            //httpClient.close()
            httpClient.getConnectionManager.shutdown()
          }
        }catch {
          case e: IOException => println("IOException caught: " + e)
        }
      }

      //return result
    }

    def doPost_str (url:String,map:Map[Int, JSONObject],charset:String, sql:String) = {
      //var httpClient:org.apache.http.impl.client.CloseableHttpClient = null
      var httpPost:HttpPost = null
      var result:String = null
      val st = System.currentTimeMillis()
      //val config = RequestConfig.custom().setConnectTimeout(60000).setSocketTimeout(15000).build()

      //++
      val httpClient = new DefaultHttpClient()
      val str = st.toString
      try{
        //httpClient = HttpClients.createDefault()
        httpPost = new HttpPost(url)
        httpPost.addHeader("Content-Type","application/json;charset=UTF-8")
        httpPost.addHeader("requestid",str)
        //import java.util
        val list = ArrayBuffer[JSONObject]()
        val list2 = new ArrayList[JSONObject]()

        val iterator = map.iterator
        while (iterator.hasNext) {
          val elem = iterator.next()
          //list.append(elem._2)
          list2.add(elem._2)
        }
        //println(list.toString())
        println("requestid" + st)
        println(list2.toString())

        if(list2.size > 0){
          //val entity = new StringEntity("{\"data\":"+list.toString+"}")
          val entity = new StringEntity("{\"data\":"+list2.toString+"}", "UTF-8")
          entity.setContentEncoding("UTF-8")
          entity.setContentType("application/json")
          httpPost.setEntity(entity)
        }
        val response = httpClient.execute(httpPost)
        val httpEntity = response.getEntity
        println(EntityUtils.toString(httpEntity, "UTF-8"))

        val code = response.getStatusLine.getStatusCode
        println("getStatusCode----------------" + code)
        if (code == 200) {
          var connection2 : Connection = null
          try {
            //线上mysql
            connection2 = DriverManager.getConnection("jdbc:mysql://172.18.4.3:3306/neworiental_v3?useSSL=false", "admin", "qq9527@xdf")
            val ps : PreparedStatement = connection2.prepareStatement(sql)
            ps.execute()
            ps.close()
          } catch {
            case e => println("comm err "+e)
          }
          connection2.close()
        }

        httpPost.releaseConnection()
        //httpClient.close()
        httpClient.getConnectionManager.shutdown()
      }catch{
        case e: Exception => println("exception caught: " + e)
        case c: ClientProtocolException => println("ClientProtocolException caught: " + c)
      }finally {
        try {
          if (httpPost != null) {
            httpPost.releaseConnection()
          }
          if (httpClient != null) {
            //httpClient.close()
            httpClient.getConnectionManager.shutdown()
          }
        }catch {
          case e: IOException => println("IOException caught: " + e)
        }
      }

      //return result
    }


    //    hiveCtx.sql("""drop table test.bi_help_student""".stripMargin)
    //    val now = new Date()
    //    val dateFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    //    val date = dateFormat.format(now)
    //    println(date)
    //    println(new SimpleDateFormat("HH:mm:ss").format(new Date()))
    //
    //
    //
    //    if (date.toInt >= 9 && date.toInt <= 21) {
    //      println(now)
    //    }




    //    //mysql测试
    //    val t_sql3_4 = "truncate table test.canal_test_table"
    //    val sql3_4 = "insert into test.canal_test_table (name, age, city) VALUES (\"hh\", 7, \"ff\")"
    //
    //    var connection : Connection = null
    //    try {
    //      connection = DriverManager.getConnection("jdbc:mysql://172.18.4.2:3306/test?useSSL=false", "admin", "qq9527@xdf")
    //      val ps : PreparedStatement = connection.prepareStatement(t_sql3_4)
    //      ps.execute()
    //      ps.close()
    //      val ps2 : PreparedStatement = connection.prepareStatement(sql3_4)
    //      ps2.execute()
    //      ps2.close()
    //    } catch {
    //      case e => println("comm err "+e)
    //    }
    //    connection.close()
    //
    //    val sqlContext = new SQLContext(sc)
    //          val h3df = sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://172.18.4.2:3306/test",
    //            "driver" -> "com.mysql.jdbc.Driver",
    //            "dbtable" -> "(select name,age,city from test.canal_test_table) as h3",
    //            "user" -> "admin","password" -> "qq9527@xdf")).load()
    //    val properties = new Properties()
    //          properties.setProperty("user", "admin")
    //          properties.setProperty("password", "qq9527@xdf")
    //    //线上
    //          h3df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://172.18.4.2:3306/test", "canal_test_table_h5", properties)

    sc.stop()
    //    val aa = 0.8811
    //    val bb = Math.round(aa*100)/100.0
    //    println(bb)

  }



}
