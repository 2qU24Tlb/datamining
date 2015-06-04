package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.RangePartitioner


// start of data structure
class VertItem(item: Array[Long], TIDs: Array[Long]) extends Serializable {
  val _item = item.sorted
  val _TIDs = TIDs.sorted
  val prefix = (_item.take(_item.length - 1).mkString)

  def +(another: VertItem) = {
    new VertItem(this._item, (this._TIDs ++ another._TIDs).distinct.sorted)
  }
  def intersect(another: VertItem) = {
    new VertItem((this._item ++ another._item).distinct.sorted,
      (this._TIDs.intersect(another._TIDs)).sorted)
  }
  override def toString(): String = {
    "(" + _item.mkString(",") + ")" + ":" + _TIDs.mkString(",")
  }
}

object Utils {
  var debug = false
  var result = ArrayBuffer[String] ()

  // check partition status
  def checkParttion[T] (part: RDD[T]) {
    part.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)
  }

  def addResult(item: String) {
    if (debug)
      result.append(item)
  }

  def showResult() {
    if (debug)
      for (i <- result)
        println(i)
  }

  //function: write result to log file
}

// start of main program
class SVTDriver(transactions: RDD[Array[Long]], minSup: Double) extends Serializable {
  val _dbLength: Long = transactions.count
  val _rminSup: Long = (minSup * _dbLength).ceil.toLong

  def run() {
    println("Number of Transactions: " + _dbLength.toString)

    // level 1
    val _freqItems = genFreqItems(transactions, _rminSup).cache
    if (Utils.debug)
      _freqItems.collect.foreach(x => Utils.addResult(x.toString))

    // level 2
    val _freqEClass = genFreqEclass(_freqItems, _rminSup).cache
    if (Utils.debug)
      _freqEClass.collect.foreach(x => Utils.addResult(x.toString))

    // level 3
    val _freqSets = genFreqSets(_freqEClass, _rminSup).cache
    if (Utils.debug)
      _freqSets.collect.foreach(x => Utils.addResult(x.toString))

    Utils.showResult
  }

  //generate local frequent items in each partition
  def genFreqItems(DB: RDD[Array[Long]], rminSup: Long): RDD[VertItem] = {
    val _localItems = DB.mapPartitions(genItems).cache
    val _globalItems = _localItems.map(x => (x._1, x._2.length))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .collect
      .map(_._1)
    val localResult =  _localItems.filter(x => _globalItems.contains(x._1))
      .map(x => new VertItem(Array(x._1), x._2))
    localResult
  }

  // generate first level of equivalent class from singletons
  def genFreqEclass(singletons: RDD[VertItem], rminSup: Long): RDD[VertItem] = {
    val _localEclass  = singletons.mapPartitions(genEclass).cache
    val _globalItems = _localEclass.map(x => (x._item.mkString, x._TIDs.length))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .collect
      .map(_._1)
    val localFrequent =  _localEclass.filter(
      x => _globalItems.contains(x._item.mkString))

    // [BugFix] we need to calculate the size of freqEclass that fits the memory
    val reMap = localFrequent.keyBy(x => x.prefix.mkString)
    val localResult = reMap.partitionBy(
      new RangePartitioner[String, VertItem](singletons.count.toInt, reMap)).map(_._2)

    localResult
  }

  // generate inheritors from equivalent class
  def genFreqSets(Eclass: RDD[VertItem], rminSup: Long): RDD[VertItem] = {
    val _localEclass  = Eclass.mapPartitions(eclat)

    // [BugFix] calculate the density of database

    _localEclass
  }

  // transform horizontal database to vertical form
  def genItems(iter: Iterator[Array[Long]]) : Iterator[(Long, Array[Long])] = {
    val _item2TID = HashMap.empty[Long, ArrayBuffer[Long]]

    var cur = Array[Long]()
    while (iter.hasNext) {
      cur = iter.next
      for (i <- cur.tail) {
        if (_item2TID.contains(i))
          _item2TID(i).append(cur.head)
        else
          _item2TID  += (i -> ArrayBuffer(cur.head))
      }
    }
    val _result = _item2TID.toList
      .sortBy(x => x._1)
      .map(x => (x._1, x._2.toArray))
      .iterator

    _result
  }

  def genEclass(iter: Iterator[VertItem]): Iterator[VertItem] = {
    val vList = iter.toList
    var i, j = 0

    var result = for (i <- 0 to vList.length - 1;
      j <- i + 1 to vList.length - 1;
      if (vList(i).prefix == vList(j).prefix)) yield {

      vList(i).intersect(vList(j))
    }
    result.filter(x => x._TIDs.length != 0)
    result.iterator
  }

  def eclat(iter: Iterator[VertItem]): Iterator[VertItem] = {
    val vList = iter.toList
    var i, j = 0
    
    var result = for (i <- 0 to vList.length - 1;
      j <- i + 1 to vList.length - 1;
      if (vList(i).prefix == vList(j).prefix)) yield {

      vList(i).intersect(vList(j))
    }
    result.filter(x => x._TIDs.length != 0)
    result.iterator

  }

  // declat

}

// start of test
object MyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scale Vertical Mining example")
    val sc = new SparkContext(conf)

    val DB = sc.textFile(args(0)).map(_.split("\\s+").map(_.toLong)).cache
    val minSup = args(1).toDouble

    // val DB = sc.textFile("/tmp/hao/DB/retail.txt").map(_.split("\\s+").map(_.toLong)).cache
    // val minSup = 0.5

    val model = new SVTDriver(DB, minSup)
    Utils.debug = true
    model.run()

    sc.stop
  }
}
