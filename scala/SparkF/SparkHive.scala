package SparkF

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ztf on 2017/8/4.
  */
object SparkHive {

  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("Spark Hive")
    val sc= new SparkContext(conf)
    val hc=new   HiveContext(sc)
    val sql_str1="drop table If   Exists neworiental_report.question_to_paper"
    hc.sql(sql_str1)
    val sql_str2="""create table neworiental_report.question_to_paper as
                   |select eq.id as id
                   |,case when hpt.type_name is not null then  hpt.type_name
                   | else "null" end paper_type,
                   |concat(hpy.year,hpn.prov,hpn.city,hpn.county,hpt.type_name) as paper_name
                   |from neworiental_v3.entity_question eq
                   |left join
                   |(SELECT h2.id  as id ,h2.type_name as type_name
                   |from(
                   |select h1.id as id,h1.type_name as type_name,
                   |row_number() over(partition by h1.id order by h1.type_num desc ) row
                   | from (
                   |select eq.id as id ,eep.type_name as type_name,
                   |case when eep.type_name in ("高考真题","中考真题","高考") then 7
                   |         when eep.type_name in ("高考模拟","中考模拟") then 6
                   |         when eep.type_name in ("期末考试") then 5
                   |         when eep.type_name in ("期中考试") then 4
                   |         when eep.type_name in ("会考卷","月考卷") then  3
                   |         when eep.type_name in ("单元测试","同步测试") then  2
                   |         when eep.type_name regexp "普通高等学校招生全国统一考试|高考理科数学（全国卷|高考数学（全国|高考数学北京|中考真题|初中毕业考试" then 7
                   |         when eep.type_name regexp "高考数学模拟|一模|二模|模拟" then 6
                   |         when eep.type_name regexp "期末数学|毕业班质量检测|综合适应训练|期末" then 5
                   |         when eep.type_name regexp "期中" then 4
                   |         when eep.type_name regexp "开学|质检|会考|联考|摸底|段考" then 3
                   |         when eep.type_name regexp "周测|周练|同步|当堂|单元测试|单元训练|专项训练|一轮复习|一轮课时" then 2
                   |         when eep.type_name is not null and eep.type_name!=" " and eep.type_name!="null" then 1
                   |         else 0 end  type_num from
                   |neworiental_v3.entity_question eq
                   |join neworiental_v3.link_exercise_question leq on leq.question_id=eq.id
                   |join neworiental_v3.entity_exercise_paper eep on leq.exercise_id=eep.exercise_id
                   |where eq.subject_id<=18 and  eq.state="ENABLED"
                   |AND eq.new_format=1 AND eq.parent_question_id=0
                   |--and eq.id=576
                   |GROUP BY eq.id,eep.type_name
                   |)h1
                   |)h2 where h2.row=1) hpt on  hpt.id=eq.id
                   |left join
                   |(SELECT h2.id as id,h2.prov as prov,h2.city as city,h2.county as county
                   |from(
                   |select h1.id as id,h1.prov as prov,h1.city as city,h1.county as county,
                   |row_number() over(partition by h1.id order by h1.type_num desc ) row
                   | from (
                   |select eq.id as id,eep.prov as prov,eep.city as city,eep.county as county,
                   |case     when eep.prov regexp "全国" then 4
                   |         when eep.prov regexp "北京|天津|上海|重庆|广东|福建|湖北|海南|山东|湖南|浙江|陕西|江苏|安徽|四川" then 3
                   |         when eep.prov regexp "河北|黑龙江|贵州|甘肃|河南|山西|内蒙|新疆|广西|辽宁|青海|吉林|宁夏|云南|江西|西藏" then 2
                   |         when eep.prov is not null and eep.prov!=" " and eep.prov!="null" then 1
                   |         else 0 end  type_num from
                   |neworiental_v3.entity_question eq
                   |join neworiental_v3.link_exercise_question leq on leq.question_id=eq.id
                   |join neworiental_v3.entity_exercise_paper eep on leq.exercise_id=eep.exercise_id
                   |where eq.subject_id<=18 and  eq.state="ENABLED"
                   |AND eq.new_format=1 AND eq.parent_question_id=0
                   |--  and eq.id=576
                   |GROUP BY eq.id,eep.prov,eep.city,eep.county
                   |)h1
                   |)h2 where h2.row=1) hpn on hpn.id=eq.id
                   |left join
                   |(select eq.id as id,max(eep.year) as year from
                   |neworiental_v3.entity_question eq
                   |join neworiental_v3.link_exercise_question leq on leq.question_id=eq.id
                   |join neworiental_v3.entity_exercise_paper eep on leq.exercise_id=eep.exercise_id
                   |where eq.subject_id<=18 and  eq.state="ENABLED"
                   |AND eq.new_format=1 AND eq.parent_question_id=0
                   |GROUP BY eq.id) hpy on hpy.id=eq.id""".stripMargin
      hc.sql(sql_str2)
   sc.stop()
  }

}
