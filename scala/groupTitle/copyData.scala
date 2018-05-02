package groupTitle

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object copyData {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("copyData")
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
    val str_into_sqs=""" insert into canal.sf_question_score select * from test.sf_question_score """.stripMargin
    sql(str_into_sqs)
    val str_into_sltq=""" insert into canal.sf_link_topic_question select * from test.sf_link_topic_question  """.stripMargin
    sql(str_into_sltq)
    val str_into_sltc=""" insert into canal.sf_link_topic_cnt select * from test.sf_link_topic_cnt  """.stripMargin
    sql(str_into_sltc)
    val str_into_ssen=""" insert into canal.sf_student_exercise_new select * from test.sf_student_exercise_new  """.stripMargin
    sql(str_into_ssen)
    val str_into_ssenc=""" insert into canal.sf_student_exercise_new_cnt select * from test.sf_student_exercise_new_cnt  """.stripMargin
    sql(str_into_ssenc)
    val str_into_sqo="""  insert into canal.sf_question_out select * from test.sf_question_out """.stripMargin
    sql(str_into_sqo)
    sc_1.stop()
  }
}
