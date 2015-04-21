package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
//import org.apache.spark.rdd.RDD
import scala.collection._
import myAlg.spark.svt._

def checkFrequent(FList: List((String, Int)), item: Int): Boolean= {

}


object SVT {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile ("hdfs:///data/A1.txt")
    val minSup = 0.5
    val fileSize = file.count
    // get global frequent list, col 1 is transaction id
    val FItemsL1 = file.flatMap(line => line.split(" ").drop(1).map(item=>(item, 1)))
      //.reduceByKey(_ + _).filter(_._2 >= minSup * fileSize).sortBy(_._1).cache
      .reduceByKey(_ + _).sortBy(_._1).cache

    // stage 2 generate local frequent 2 itemsets
    val candidates = file.map(line => line.split(" ").drop(1).toSet

    // stage 3 local vertical mining


    //counts.saveAsTextFile("hdfs:///output/results")

    sc.stop()
  }
}
