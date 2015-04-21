package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection._
import myAlg.spark.svt._

class FreqModel(support:Double, file:SparkContext.textFile) {
  var globalFreqSets: Array[Int] = Array()
  var minSup = support

  def setSup(sup: Double) {minSup = sup}
  def setFreqs(freqs: List[Int]) {
    globalFreqSets = freqs.toArray
  }
  def isFreq(item: Int) :Boolean={
    false
  }
}

object SVT {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf )

    val file = sc.textFile ("hdfs:///data/A1.txt")

    val transactions  = file.map(line => line.split(" "))
    //val HsetsList = transactions.map(trans => new HSets(trans))
      //.map(word => (word, 1))
      //.reduceByKey(_ + _)

    //counts.saveAsTextFile("hdfs:///output/results")

    sc.stop()
  }
}
