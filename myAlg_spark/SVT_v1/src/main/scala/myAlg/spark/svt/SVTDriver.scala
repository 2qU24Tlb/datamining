/***
 This is for COMP 7860 course project. 
 Scalabe Vertical Mining for Big Data
 author: Hao Zhang
 email: zhangh15@myumanitoba.ca
 ***/
package myAlg.spark.svt_v1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import org.apache.spark.RangePartitioner

object MyTest {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile (args(0))
    val minSup = args(1).toDouble
    val fileSize = file.count
    val realminSup = minSup * fileSize
    //var results = "hdfs:///output/result"

    // stage 1: obtain global frequent list, col 1 is transaction id
    val FItemsL1 = file.flatMap(line => line.split(" ")
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
    val candidates = file.flatMap(line => (line.split(" ")
      .tail
      .filter(BTVal.value.contains(_))
      .sorted
      .combinations(2)
      .map(i => (i.toList, List(line(0).asDigit)))))
      .reduceByKey((x, y) => (x ++ y).distinct.sorted)

    // stage 3: re-partition & local vertical mining
// [FixMe] Calculate the size of equivalent class to make sure that fit the memory
    val newGroup = candidates.keyBy(_._1(0))
//or we should use repartitionAndSortWithinPartitions ?
    val GroupedCands = newGroup.
      partitionBy(new RangePartitioner[String, (List[String], List[Int])]
        (BTVal.value.size, newGroup)).values
    // we can check the partition status by using: 
    //GroupedCands.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)

    var EQClass = GroupedCands.
      //mapPartitions(genCandidates, preservesPartitioning = true)
      mapPartitions(genCandidates)
      .filter(_._2.length >= minSup * fileSize).cache

// [FixMe] try declat
    while (EQClass.count > 1) {
      EQClass = EQClass.map(
        key => (key._1.take(key._1.length - 1), List((key._1.last, key._2))))
        .reduceByKey(_ ++ _)
        .filter(_._2.length >= 2)
        .flatMap(x => eclat(x._1, x._2))
        .filter(x => x._2.length  >= minSup * fileSize).cache
      //EQClars.saveAsTextFile(results)
    }

    sc.stop()
  }

//[FiXMe] Can we just use Array instead of Iterator? 
  def genCandidates(iter: Iterator[(List[String], List[Int])]) :
      Iterator[(List[String], List[Int])] = {
    val LList = iter.toArray
    var i, j = 0
    val result = for (i <- 0 to LList.length - 1; j <- i + 1 to LList.length - 1) yield
      (((LList(i)_1)++(LList(j)_1)).distinct.sorted,
        (LList(i)_2).intersect(LList(j)_2).distinct.sorted)

    result.iterator
  }

  // key -> prefix
  // value -> [Itemsets, tids]
  def eclat(key: List[String], value: List[(String, List[Int])]) :
      List[(List[String], List[Int])] = {
    var i, j = 0
    val result = for (i <- 0 to value.length - 1; j <- i + 1 to value.length - 1) yield
      ((key ++ List(value(i)._1) ++ List(value(j)._1)).sorted,
        (value(i)._2.intersect(value(j)._2)).distinct.sorted)
    result.toList
  }

  def declat(key: List[String], value: List[(String, List[Int])]) :
      List[(List[String], List[Int])] = {
    var i, j = 0
    val result = for (i <- 0 to value.length - 1; j <- i + 1 to value.length - 1) yield
      ((key ++ List(value(i)._1) ++ List(value(j)._1)).sorted,
        (value(i)._2.intersect(value(j)._2)).distinct.sorted)
    result.toList
  }

}
