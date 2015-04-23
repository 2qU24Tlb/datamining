package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import myAlg.spark.svt._


object SVT {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile ("hdfs:///data/A1.txt")
    val minSup = 0.5
    val fileSize = file.count
    //var results: String = Nil

    // stage 1: obtain global frequent list, col 1 is transaction id
    val FItemsL1 = file.flatMap(line =>
      line.split(" ")
        .drop(1)
        .map(item=>(item, 1)))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSup * fileSize)
      .collectAsMap
    // method 2---> .reduceByKey(_ + _).filter(_._2 >= minSup * fileSize).sortBy(_._1).cache

    // method1: broadcast 1 level support list
    val BTVal = sc.broadcast(FItemsL1)
    // method2: Cache the frequent list without using a hashtable
    // use rdd operation lookup() instead

    // stage 2: generate frequent 2 item sets
    val candidates = file.flatMap(line =>
      (line.split(" ")
        .tail
        .filter(BTVal.value.contains(_))
        .sorted
        .combinations(2)
        .map(i => (i(0) + i(1), line(0).toString))))
      .reduceByKey((x, y) => (x+y).distinct.sorted).cache

    // stage 3: re-partition & local vertical mining
// [FixMe] Calculate the size of equivalent class
    val newGroup = candidates.keyBy(key => key._1.head)
    val GroupedCands = newGroup.partitionBy(new RangePartitioner[Char, (String,String)]
      (BTVal.value.size, newGroup)).values
    //or we should use repartitionAndSortWithinPartitions ?

    // we can check the partition status by using: 
    // LCands.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)

    var counts = GroupedCands.mapPartitions(genCandidates, preservesPartitioning = true)

    // keep generating the frequent items
    // while (counts.size != 1) {
      //counts = GroupedCands.mapPartitions(genCandidates, preservesPartitioning = true)
    //}
    counts.collect

    //counts.saveAsTextFile("hdfs:///output/results")

    sc.stop()
  }

  def genCandidates(iter: Iterator[(String, String)]) : Iterator[(String, String)] = {
    val LList = iter.toArray
    val LLength = LList.length 
    var i, j = 0

    val result = for (i <- 0 to LLength; j <- i + 1 to LLength) yield
      (((LList(i)_1)+(LList(i)_1)), ((LList(i)_2)+(LList(i)_2)))

    result.iterator
  }

}
