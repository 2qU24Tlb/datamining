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
    val fileSize = file.count.cache
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
    val GCands = candidates.keyBy(key => key._1.head)
    val LCands = GCands.partitionBy(
      new RangePartitioner[Char, (String,String)](BtVal.value.size, var2)).values
    // List all partitions 
    // LCands.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)

    LCands.mapPartitions(myfunc)

    //counts.saveAsTextFile("hdfs:///output/results")

    sc.stop()
  }

  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next;
      res .::= (pre, cur)
      pre = cur;
    }
    res.iterator
  }
}
