package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import myAlg.spark.svt._


object SVT {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile ("hdfs:///data/A1.txt")
    val minSup = 0.5
    val fileSize = file.count
    // stage 1: obtain global frequent list, col 1 is transaction id
    val FItemsL1 = file.flatMap(line => line.split(" ").drop(1).map(item=>(item, 1)))
      .reduceByKey(_ + _).collect.map(i => (i._1, i._2)).toMap.cache

    // method1: broadcast 1 level support list
    val BTVal = sc.broadcast(FItemsL1)
    // method2: Cache only the frequent list
    //.reduceByKey(_ + _).filter(_._2 >= minSup * fileSize).sortBy(_._1).cache

    // stage 2: generate local frequent 2 item sets
    for (line <- file) {
      val Hid = new HSets(line)
      val tid = Hid.tid
      val LFreqs = Hid.findFSets(BTVal.value, minSup * fileSize)
    }

    // stage 3: local vertical mining


    //counts.saveAsTextFile("hdfs:///output/results")

    sc.stop()
  }

}
