package SparkF

/*import breeze.math.Coordinated
import org.apache.calcite.util.Benchmark.Statistician
import org.apache.spark.mllib.linalg.distributed._*/
/*import com.sun.xml.bind.v2.runtime.output.MTOMXmlOutput
import org.apache.spark.mllib.linalg.{Matrices, Vectors}*/
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._
/*import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}*/
/*import org.apache.spark.mllib.stat.Statistics*/
/*import org.apache.spark.mllib.util.MLUtils*/

/**
  * Created by ztf on 2017/8/30.
  */
object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test").set("spark.testing.memory", "5368709120")
    val sc = new SparkContext(conf)
    val randNum = normalRDD(sc , 100)
       randNum.foreach(println)
  /*       /*val vd = Vectors.dense(1,2,3,4,5)
    val vdResult = Statistics.chiSqTest(vd)
    println(vdResult)
    println("--------------------------------")
    val mtx = Matrices.dense(3 , 2, Array(1,3,5,2,4,6))
    val mtxResult = Statistics.chiSqTest(mtx)
    println(mtxResult*/)
   /* val conf = new SparkConf()
      .setMaster("local")
      .setAppName("test")
      .set("spark.testing.memory", "5368709120")
    val  sc = new SparkContext(conf)
    val  data = sc.textFile("C:\\Users\\ztf\\Desktop\\a.txt")
      .map(row => {
        if(row.length == 3)
          (row ,1)
        else (row,2)
      })
    val fractions: Map[String,Double] = Map{"aa" -> 2}
    val approxSample = data.sampleByKey(withReplacement = false,fractions,0)
    approxSample.foreach(println)*/
    /*val  rddX=sc.textFile("C:\\Users\\ztf\\Desktop\\x.txt")
      .flatMap(_.split(' ').map(_.toDouble))
    val  rddY=sc.textFile("C:\\Users\\ztf\\Desktop\\y.txt")
      .flatMap(_.split(' ').map(_.toDouble))
    val correlation: Double = Statistics.corr(rddX, rddY, "spearman")
    println(correlation)*/

    /*val rdd=sc.textFile("C:\\Users\\ztf\\Desktop\\a.txt")
      .map(_.split(' ').map(_.toDouble))
      .map(line => Vectors.dense(line))
    val summary = Statistics.colStats(rdd)
    println(summary.mean+"平均值")
    println(summary.variance + "标准差")
    println(summary.normL1+"哈曼段距离")
    println(summary.normL2 + "欧几里得距离")*/
      //.map(vue => (vue(0).toLong,vue(1).toLong,vue(2)))
      //.map(vue2 => new MatrixEntry(vue2 _1,vue2 _2,vue2 _3))
   // val crm=new CoordinateMatrix(rdd)
   // println(crm.entries.foreach(println))
   /* val rm= new RowMatrix(rdd)
    println(rm.numRows()+"行数")
    println(rm.numCols()+"列数")*/

    /*val mx =Matrices.dense(2,3,Array(1,2,3,4,5,6))
    println(mx)*/
    /* val conf = new SparkConf().setAppName("test").setMaster("local").set("spark.testing.memory", "5368709120")
     val sc = new SparkContext(conf)
     val  mu = MLUtils.loadLibSVMFile(sc,"C:\\Users\\ztf\\Desktop\\a.txt")
    mu.foreach(print)*/
    /*val vd:  Vector = Vectors.dense(1,0,3)
    println(vd(2))
    val vs: Vector = Vectors.sparse(5,Array(0,1,2,4),Array(9,2,5,7))
    println(vs(4))*/
   /* val conf = new SparkConf().setAppName("tets").setMaster("local[2]").set("spark.testing.memory", "5368709120")
    val sc = new SparkContext(conf)
    val arr = sc.parallelize(Array(1,2,3,4,5))
    val result = arr.aggregate(6)(math.max(_, _) , _ + _)
    println("--------------")
    println(result)*/*/
  }
}
