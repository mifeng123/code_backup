package SparkF


import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ztf on 2017/8/8.
  */
object SparkML {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkML")
    val sc = new SparkContext(conf)

    val spam = sc.textFile("/zkl/spam/2*.txt")
    val normal = sc.textFile("/zkl/normal/2*.txt")
    val tf = new HashingTF(numFeatures = 10000)

    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))

    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = negativeExamples.union(negativeExamples)
    //trainingData.cache()

    val model = new LogisticRegressionWithSGD().run(trainingData)

    val  posTest = tf.transform(
      "O M G GHT cheap stuff by Spark sending money to   ...".split(" ")
    )
    val negTest= tf.transform(
      "Hi Dad , I started  studing Spark the other ".split(" ")
    )

    println("Prediction for positive test example: " + model.predict(posTest))
    println("Prediction for negative test example: " + model.predict(negTest))
  }
}
