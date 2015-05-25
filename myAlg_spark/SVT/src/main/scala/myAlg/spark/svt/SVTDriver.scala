package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

// start of data structure
class VertItem(_item: Array[BigInt], _TIDs: Array[BigInt]) {
  def item = _item
  def TIDs = _TIDs
  def prefix(): Array[BigInt] = _item.take(_item.length - 1)
}

// start of main program
class SVTDriver(val DB: RDD[Array[BigInt]], val minSup: Double) {
  val _dbLength = DB.count
  val _rminSup = BigInt((minSup * _dbLength).ceil.toInt)
  var _results = Array[String]()

  def run() {
    println("Number of Transactions: " + _dbLength.toString)
    //val _freqItems = genFreqItems(DB, _rminSup)
    //val _freqItemsets = genFreqItemsets(_freqItems, _rminSup)
  }

  def genFreqItems(DB: RDD[Array[BigInt]], rminSup: BigInt): Array[VertItem]={
   // get local singleton list count
    val localItems = DB.mapPartitions(genVertItem)
    val globalFrequent = localItems.map(x => (x.item, x.TIDs.size)).reduceByKey(_ + _).filter(_._2 >= rminSup)
    localItems.filter(x => x.items in globalFrequent)
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

  def fakeData() {
    val a = Array(1, 1, 2, 3, 4, 5).map(BigInt(_));
    val b = Array(2, 1, 2, 3).map(BigInt(_));
    val c = Array(3, 1, 4).map(BigInt(_));
    //val fDB = sc.parallelize(Array(a, b, c));
  }

  def genVertItem(iter: Iterator[Array[BigInt]]) : Iterator[VertItem] = {
    val _item2TID = scala.collection.mutable.HashMap.empty[BigInt, ArrayBuffer[BigInt]]

    val cur = Array[BigInt]()
    while (iter.hasNext) {
      cur = iter.next
      for (i <- cur.tail) {
        if (_item2TID.contains(i)) 
          _item2TID(i).prepend(cur.head)
        else
          _item2TID  += (i -> (i->ArrayBuffer(cur.head))) 
      }
    }
    var result: ArrayBuffer[VertItem] = ArrayBuffer(VertItem).empty
    _item2TID.foreach {keyVal => VertItem(keyVal._1, keyVal._2)}
  }
}

// start of test
object MyTest extends App {
  val conf = new SparkConf().setAppName("Scale Vertical Mining example")
  val sc = new SparkContext(conf)

  val DB = sc.textFile(args(0)).map(_.split(" ").map(_.toInt)).cache
  val minSup = args(1).toDouble

  val model = new SVTDriver(DB, minSup)
  model.run()

  sc.stop
}
