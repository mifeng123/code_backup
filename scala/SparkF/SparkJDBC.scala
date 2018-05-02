package SparkF

import java.sql.{DriverManager, ResultSet}


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD

/**
  * Created by ztf on 2017/8/9.
  */
object SparkJDBC {

  /*def extractValues(r: ResultSet) = {
     (r.getInt(1),r.getInt(2),r.getString(3),r.getString(4),r.getString(5),r.getInt(6),r.getDate(7))
  }*/
  def extractValues(r: ResultSet) = {
   // (r.getInt("id"),r.getString("json_data"))

    (r.getInt("id"),r.getString("json_data"),r.getInt("struct_id"),r.getInt("subject_id"),r.getInt("type_id"))
  }

  def main(args: Array[String]): Unit = {
    /* val arry = Array("第一个","第二个","第三个","第三个","第四个","第五个","第六个")
       val arry_1 = arry(3)
       println(arry.apply(0))
       val str = """开始拼接"""+arry_1+"""结束拼接"""
       println(str)*/
    /*val topics = List(("pands", 1),("logs", 1)).toMap  471859200
       println(topics+"结束")*/
    val conf = new SparkConf().setAppName("Spark jdbc")
      .setMaster("local[2]").set("spark.testing.memory", "5368709120")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    /*
    val hc=new   HiveContext(sc)
    val sqlc= new SQLContext(sc)
    val url = "jdbc:mysql://172.18.4.2:3306/test?user=reader&password=1a2s3dqwe"
    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    val df = sqlc.read.jdbc(url , "test.code_region",prop)
    println("数据"+df.select())*/
/*    val sqlc = new SQLContext(sc).jdbc()*/
    val sql= """select b.id id ,c.struct_id struct_id,b.subject_id subject_id,c.type_id type_id , b.json_data json_data from  neworiental_v3.entity_question b
               |JOIN neworiental_v3.entity_question_type c ON b.question_type_id=c.type_id
               |where b.state="ENABLED" AND b.new_format=1 AND b.parent_question_id=0 and (b.id > ? or b.id < ?) limit 10 """.stripMargin
  val data = new JdbcRDD(sc ,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://10.10.6.7:3306/neworiental_v3?useUnicode=true&characterEncoding=UTF-8", "reader", "1a2s3dqwe")
      }
      ,sql ,
      lowerBound = -1, upperBound = 1, numPartitions =  2,mapRow = extractValues).cache()
    data.collect().toList.foreach{x => println(x)}
    //println(data.collect().toList.tail)
//    data.saveAsTextFile("/zkl/dir/kk")
/*    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val dataFream = data.toDF()
    dataFream.registerTempTable("Data")
    sqlContext .sql("select * from Data").collect.foreach(println)*/
//    val data_num = data.collect().toList.map(x => x)
//    data_num.toDF()insertInto("test.kk")

    /*   data.collect().toList.foreach(println)*/

  /*  var   num  = data.collect().toIterator
    while(num.hasNext){
   num.map(x => println(x + "数据"))
    }*/
    sc.stop()
  }
}
