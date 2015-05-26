package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

// start of data structure
class VertItem(item: Array[BigInt], TIDs: Array[BigInt]) extends java.io.Serializable {
  val _item = item.sorted
  val _TIDs = TIDs.sorted
  def getItem = _item
  def getTIDs = _TIDs
  def prefix(): Array[BigInt] = {
    if (_item.length == 1)
      _item
    else
      _item.take(_item.length - 1)
  }
}

// start of main program
class SVTDriver(DB: RDD[Array[BigInt]], minSup: Double) extends java.io.Serializable {
  val _dbLength = DB.count
  val _rminSup = BigInt((minSup * _dbLength).ceil.toInt)
  var _results = Array[String]()

  def run() {
    println("Number of Transactions: " + _dbLength.toString)
    val _freqItems = genFreqItems(DB, _rminSup)
    //val _freqItemsets = genFreqItemsets(_freqItems, _rminSup)
  }

  def genFreqItems(DB: RDD[Array[BigInt]], rminSup: BigInt): Array[VertItem]={
    val _localItems = DB.mapPartitions(genVertItem)
    // val globalFrequent = localItems.map(x => (x.item, x.TIDs.size))
    //   .reduceByKey(_ + _)
    //   .filter(_._2 >= rminSup)
    // localItems.filter(x => x.items in globalFrequent)
    _localItems.collect
  }

  def genFreqItemsets(freqitems: Array[VertItem], rminSup: BigInt): Array[VertItem]={
    var tmp = new Array[VertItem](1)
    tmp
  }

  def addToResults(items: Array[VertItem]) {
  }

  def showResults() {
    _results.foreach(println)
  }

  // generate vertical domain items in each partition
  def genVertItem(iter: Iterator[Array[BigInt]]) : Iterator[VertItem] = {
    val _item2TID = scala.collection.mutable.HashMap.empty[BigInt, ArrayBuffer[BigInt]]

    var cur = Array[BigInt]()
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
      .map(x => new VertItem(Array(x._1), x._2.toArray))
      .iterator

    _result
  }
}

// start of test
object MyTest extends App {
  val conf = new SparkConf().setAppName("Scale Vertical Mining example")
  val sc = new SparkContext(conf)

  // val t1 = Array(1, 1, 2, 3, 4, 5).map(BigInt(_));
  // val t2 = Array(2, 1, 2, 3).map(BigInt(_));
  // val t3 = Array(3, 1, 4).map(BigInt(_));
  // val t4 = Array(4, 1, 3, 4, 5).map(BigInt(_));
  // val t5 = Array(5, 2, 3, 4, 5).map(BigInt(_));
  // val t6 = Array(6, 2, 3, 4).map(BigInt(_));
  // val t7 = Array(7, 3, 4, 5).map(BigInt(_));
  // val t8 = Array(9, 1, 2, 3).map(BigInt(_));
  // val t9 = Array(9, 2, 3, 5).map(BigInt(_));
  // val fDB = sc.parallelize(Array(t1, t2, t3, t4, t5, t6, t7, t8, t9), 3);

  val DB = sc.textFile(args(0)).map(_.split(" ").map(BigInt(_))).cache
  val minSup = args(1).toDouble

  val model = new SVTDriver(DB, minSup)
  model.run()

  sc.stop
}
