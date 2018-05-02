package SparkF

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks


/**
  * Created by ztf on 2017/8/14.
  */
object HiveJsonAnswerObject {
  def extractValues(r: ResultSet) = {
    // (r.getInt("id"),r.getString("json_data"))

    (r.getInt("id"),r.getString("json_data"),r.getInt("struct_id"),r.getInt("subject_id"),r.getInt("type_id"))
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark jdbc")
      .setMaster("local[2]").set("spark.testing.memory", "5368709120")
      //.set("spark.serializer", "org.apache .spark.serializer.KryoSerializer")
    //571,16467,1304645,898,15158,26795,2485207
    val sc = new SparkContext(conf)
    val sql= """select b.id id ,c.struct_id struct_id,b.subject_id subject_id,c.type_id type_id , b.json_data json_data from  neworiental_v3.entity_question b
               |JOIN neworiental_v3.entity_question_type c ON b.question_type_id=c.type_id
               |where b.state="ENABLED" AND b.new_format=1 AND b.parent_question_id=0 and (b.id > ? or b.id < ? ) and b.json_data is not null
               |and b.id=1032 limit 1
               """.stripMargin
    val data = new JdbcRDD(sc ,
      () => {
        Class.forName("com.mysql.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://10.10.6.7:3306/neworiental_v3?useUnicode=true&characterEncoding=UTF-8", "reader", "1a2s3dqwe")
      }
      ,sql ,
      lowerBound = -1, upperBound = -1, numPartitions =  1,mapRow = extractValues).cache()
    //println(data.collect().toList.size+"数据的 长度")
    data.collect().toList.foreach{x => evaluate(x._1,x._2,x._3,x._4,x._5) }
    sc.stop()
  }
val hiveJsonAnswerClass = new HiveJsonAnswerClass()
  def  evaluate(id: Int,json_data: String,struct_id: Int,subject_id: Int,type_id: Int): Unit = {
/*     var result2 = new Array[Int](7)*/

    var jsonObject = new JSONObject()
    try{
      jsonObject = new JSONObject(new JSONObject(json_data).get("content").toString)
    }catch {
      case e:Exception => {
        jsonObject = new JSONObject(json_data.toString)
      }
    }
   var result =" "
  try {
    if(struct_id == 1){
      result =hiveJsonAnswerClass.structOne(json_data,subject_id,type_id)
    }
    if(struct_id == 2 || struct_id == 3 || struct_id == 4){
      var bool = true
      result = hiveJsonAnswerClass.structNotOne(json_data,bool,type_id)
    }
    if(struct_id == 5 ||  struct_id == 7){

      var materialJsonArray = jsonObject.getJSONArray("material")
      if(materialJsonArray.length()>0) {
        var bool = false
        var questionsJsonArray = jsonObject.getJSONArray("questions")
        //为符合题目做准备，存放小题
        var arrayBuffer : ArrayBuffer[String] =new  ArrayBuffer[String](20)
        for (i <- 0 to  questionsJsonArray.length() - 1) {
          try {
            var samllQuestion = questionsJsonArray.get(i).toString
            var questionJsonObject = new JSONObject(samllQuestion)
            var questionType = ""
            if (subject_id == 5 || subject_id == 6) {
              try {
                questionType = questionJsonObject.get("topic_type").toString
              } catch {
                case e: Exception => {
                  questionType = questionJsonObject.get("type").toString
                }
              }
            } else {
              questionType = questionJsonObject.get("type").toString
            }
            var typeJsonObject = new JSONObject(questionType)
            var typeValue = typeJsonObject.get("name").toString.trim
            typeValue = typeValue.replaceAll("\u00A0", "")
            typeValue = typeValue.replaceAll("[^(a-zA-Z0-9\\u4e00-\\u9fa5)]", "")
            if ("选择题".equals(typeValue)) {
              result = hiveJsonAnswerClass.structOne(samllQuestion, subject_id, type_id)
              arrayBuffer += result
            }
            else {
              result = hiveJsonAnswerClass.structNotOne(samllQuestion, bool, type_id)
              arrayBuffer += result
            }
          } catch {
            case e: Exception => {
              if ("Exception".equals(e)) {
                new Breaks().break()
              }
            }
          }
        }
        var tem = 0
        var m = 0
        for(k <- 0 to arrayBuffer.length - 1){
          var temInt = arrayBuffer(k).toString.split("\\|")
          var smallAnswer = temInt(2).toInt + temInt(3).toInt
          var smallAnalysis = temInt(4).toInt + temInt(5).toInt
          if(smallAnswer == 0){
            m=k
            new Breaks().break()
          }else{
            if(smallAnalysis < tem || k == 0){
              tem = smallAnalysis
              m=k
            }
          }
        }
        val resturt_tmp  = arrayBuffer(m).toString().split("\\|")
        result =  resturt_tmp(0) + "|" + resturt_tmp(1) + "|" + resturt_tmp(2) + "|" + resturt_tmp(3) + "|" + resturt_tmp(4) + "|"+ resturt_tmp(5) + "|" + resturt_tmp(6) + "|" + resturt_tmp(7)
      }else{
        result =0 + "|" + 0 + "|" + 0 + "|" + 0 + "|" + 0 + "|"+ 0 + "|" + "解析为空" + "|" + "答案为空"
      }
      }
    if(struct_id == 6){
       var materialJsonArray = jsonObject.getJSONArray("material")
      if(materialJsonArray.length() > 0){
        var questionsJsonArray = jsonObject.getJSONArray("questions")
        var arrayBuffer = new ArrayBuffer[String](20)
        for(i <- 0 to questionsJsonArray.length()-1){
          try{
            var smallQuestion = questionsJsonArray.get(i).toString
            var  questionJsonObject = new JSONObject(smallQuestion)
            result = hiveJsonAnswerClass.structOne(smallQuestion,subject_id,type_id)
            arrayBuffer += result
          }catch{
            case e: Exception => {
              if ("Exception".equals(e)) {
                new Breaks().break()
              }
            }
          }
        }
        var tem = 0
        var m =0
    //    println(arrayBuffer.length+ "'数据到了" + arrayBuffer(0).toString + "wheuqe|rv".mkString(","))
        for(k <- 0 to  arrayBuffer.length - 1){
          var temInt = arrayBuffer(k).toString.split("\\|")
          var smallAnswer = temInt(2).toInt + temInt(3).toInt
          var smallAnalysis = temInt(4).toInt + temInt(5).toInt
          if(smallAnswer == 0){
            m=k
            new Breaks().break()
          }else{
            if(smallAnalysis < tem || k == 0){
              tem = smallAnalysis
              m=k
            }
          }

        }
        val resturt_tmp  = arrayBuffer(m).toString().split("\\|")
//        println(arrayBuffer(m))
        resturt_tmp(0) = "30"
        resturt_tmp(1) = "0"
        result =  resturt_tmp(0) + "|" + resturt_tmp(1) + "|" + resturt_tmp(2) + "|" + resturt_tmp(3) + "|" + resturt_tmp(4) + "|"+ resturt_tmp(5) + "|" + resturt_tmp(6) + "|" + resturt_tmp(7)

      }
      else{
        result =0 + "|" + 0 + "|" + 0 + "|" + 0 + "|" + 0 + "|"+ 0 + "|" + "解析为空" + "|" + "答案为空"
      }
    }

  }catch {
    case e: Exception => {if ("Exception".equals(e)) {
      println( "try出错了")
    }}
  }

//val str= new HiveJsonAnswerClass().structOne(json_data,subject_id,type_id)
   var  result_l = result.split("\\|")
   println(id+"解析的id")
   println(result_l(0) + "," + result_l(1) + "," + result_l(2) + "," + result_l(3) + "," + result_l(4) + ","+ result_l(5) + "," + result_l(6) + "," + result_l(7) +"|传过来的结果"+id+"|")
  }
}
