package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap


// start of data structure
class VertItem(item: Array[Long], TIDs: Array[Long]) extends Serializable {
  val _item = item.sorted
  val _TIDs = TIDs.sorted
  def +(another: VertItem) = {
    new VertItem(this._item, (this._TIDs ++ another._TIDs).distinct.sorted)
  }
  def intersect(another: VertItem) = {
    new VertItem((this._item ++ another._item).distinct.sorted,
      (this._TIDs.intersect(another._TIDs)).sorted)
  }
  def prefix(): Array[Long] = {
    if (_item.length == 1)
      _item
    else
      _item.take(_item.length - 1)
  }
}

// start of main program
class SVTDriver(transactions: RDD[Array[Long]], minSup: Double) extends Serializable {
  val _dbLength: Long = transactions.count
  val _rminSup: Long = (minSup * _dbLength).ceil.toLong
  // var result

  def run() {
    println("Number of Transactions: " + _dbLength.toString)
    val _freqItems = genFreqItems(transactions, _rminSup).cache
    //val _freqEClass = genFreqEclass(_freqItems, _rminSup).cache
    // _freqEClass.collect.foreach(
    //   x => println(x._item.mkString(), x._TIDs.mkString(" ")))
    // level 3: repartiotion to do local eclat/declat.
  }

  def genFreqItems(DB: RDD[Array[Long]], rminSup: Long): RDD[VertItem] = {
    val _localItems = DB.mapPartitions(genItems).cache
    val _globalItems = _localItems.map(x => (x._1, x._2.length))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .collect
      .sortBy(_._1)
      .map(_._1)
    val localFrequent =  _localItems.filter(x => _globalItems.contains(x._1))
      .map(x => new VertItem(Array(x._1), x._2))
    localFrequent
  }

  def genFreqEclass(singletons: RDD[VertItem], rminSup: Long): RDD[VertItem] = {
    //val _localEclass  = singletons.mapPartitions(genEclass).cache
    singletons
  }

  // generate vertical domain items in each partition
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
    val LList = iter.toList
    var i, j = 0

    var result = for (i <- 0 to LList.length - 1; j <- i + 1 to LList.length - 1) yield {
        LList(i).intersect(LList(j))
      }
    result.iterator
  }
}

// start of test
object MyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scale Vertical Mining example")
    val sc = new SparkContext(conf)

    // val t1 = Array(1, 1, 2, 3, 4, 5).map(_.toLong);
    // val t2 = Array(2, 1, 2, 3).map(_.toLong);
    // val t3 = Array(3, 1, 4).map(_.toLong);
    // val t4 = Array(4, 1, 3, 4, 5).map(_.toLong);
    // val t5 = Array(5, 2, 3, 4, 5).map(_.toLong);
    // val t6 = Array(6, 2, 3, 4).map(_.toLong);
    // val t7 = Array(7, 3, 4, 5).map(_.toLong);
    // val t8 = Array(8, 1, 2, 3).map(_.toLong);
    // val t9 = Array(9, 2, 3, 5).map(_.toLong);
    // val fDB = sc.parallelize(Array(t1, t2, t3, t4, t5, t6, t7, t8, t9), 3);

    val DB = sc.textFile(args(0)).map(_.split("\\s+").map(_.toLong)).cache
    val minSup = args(1).toDouble

    val model = new SVTDriver(DB, minSup)
    model.run()

    sc.stop
  }
}
 
