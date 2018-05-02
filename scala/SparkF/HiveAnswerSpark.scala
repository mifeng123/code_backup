package SparkF



import java.util

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ztf on 2017/8/23.
  */
object HiveAnswerSpark {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
        .setAppName("HiveAnswerSpark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.set("spark.sql.codegen", "true")
      //一个一个查看是否生效
         .set("spark.shuffle.consolidateFiles","true")
        .set("spark.speculation","true")
      .set("spark.task.maxFailures","8")
      .set("spark.akka.timeout","300")
      .set("spark.network.timeout","300")
      .set("spark.yarn.max.executor.failures","100")
    //一个一个查看是否生效
     // .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
     // .set("spark.sql.inMemoryColumnarStorage.batchSize", "500")
      //.set("spark.sql.parquet.compression.codec", "snappy")
       // .set("spark.default.parallelism","10")
     // .set("spark.sql.shuffle.partitions","100")
    val sc_1 = new SparkContext(sparkConf)
    val hc_1 = new HiveContext(sc_1)
    /*import  hc_1.implicits._*/
    import hc_1.sql
    val sql_str2= """create temporary function jsontest  as 'hiveJson.HiveJsonAnswer'""".stripMargin
    sql(sql_str2)
    val sql_str3="""drop table if exists test.answer_text""".stripMargin
    sql(sql_str3)
    val sql_str4="""create table  test.answer_text(qid bigint,sum array<String>) row format delimited FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' stored as textfile""".stripMargin
    sql(sql_str4)
    val sql_str5="""insert overwrite table test.answer_text select a.qid,split(jsontest(cast(a.qid as int),a.con,c.struct_id,cast(b.subject_id as int),c.type_id), ",") from question.question_json a JOIN neworiental_v3.entity_question b ON a.qid=b.id JOIN neworiental_v3.entity_question_type c ON b.question_type_id=c.type_id where b.state="ENABLED" AND b.new_format=1 AND b.parent_question_id=0""".stripMargin
    sql(sql_str5)

    sc_1.stop()

  }
}
