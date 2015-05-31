package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

// start of main program
class SVTDriver(transactions: RDD[Array[Long]], minSup: Double) extends Serializable {
  val _dbLength: Long = transactions.count
  val _rminSup: Long = (minSup * _dbLength).ceil.toLong
  // var result

  def run() {
    println("Number of Transactions: " + _dbLength.toString)
    val _freqItems = genFreqItems(transactions, _rminSup)
    // level 2: intersection with local singletons to get 2-frequent itemset.
    // level 3: repartiotion to do local eclat/declat.
  }

  def genFreqItems(DB: RDD[Array[Long]], rminSup: Long): RDD[(Long, Array[Long])] = {
    val _localItems = DB.mapPartitions(genVertItem)
    val _globalItems = _localItems.map(x => (x._1, x._2.length))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .collect.sortBy(_._1).map(_._1)
    val localFrequent =  _localItems.filter(x => _globalItems.contains(x._1))
    localFrequent
  }

  // generate vertical domain items in each partition
  def genVertItem(iter: Iterator[Array[Long]]) : Iterator[(Long, Array[Long])] = {
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
}

object SVTDriver{
  // start of data structure
  class VertItem(item: Array[BigInt], TIDs: Array[BigInt]) extends Serializable {
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
}

// start of test
object MyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scale Vertical Mining example")
    val sc = new SparkContext(conf)

    // val t1 = Array(1, 1, 2, 3, 4, 5).map(BigInt(_));
    // val t2 = Array(2, 1, 2, 3).map(BigInt(_));
    // val t3 = Array(3, 1, 4).map(BigInt(_));
    // val t4 = Array(4, 1, 3, 4, 5).map(BigInt(_));
    // val t5 = Array(5, 2, 3, 4, 5).map(BigInt(_));
    // val t6 = Array(6, 2, 3, 4).map(BigInt(_));
    // val t7 = Array(7, 3, 4, 5).map(BigInt(_));
    // val t8 = Array(8, 1, 2, 3).map(BigInt(_));
    // val t9 = Array(9, 2, 3, 5).map(BigInt(_));
    // val fDB = sc.parallelize(Array(t1, t2, t3, t4, t5, t6, t7, t8, t9), 3);

    val DB = sc.textFile(args(0)).map(_.split(" ").map(_.toLong)).cache
    val minSup = args(1).toDouble

    val model = new SVTDriver(DB, minSup)
    model.run()

    sc.stop
  }
}
