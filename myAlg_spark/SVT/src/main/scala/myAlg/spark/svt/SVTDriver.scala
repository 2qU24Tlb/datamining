package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// start of data structure
class VertItem(item: Array[BigInt], TIDs: Array[BigInt]) {
  def item = item
  def TIDs = TIDs
  def getPrefix = item.take(item.length - 1)
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
    var tmp = new Array[VertItem](1)
    tmp
    // get local singleton list count
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
    val _item2TID = scala.collection.mutable.HashMap.empty[BigInt, List[BigInt]]

    val cur = Array[BigInt]()
    while (iter.hasNext) {
      cur = iter.next
      for (i <- cur.tail) {
        if (_item2TID.contains(i)) 
          _item2TID(i).append(cur.head)
        else
          _item2TID  += (i -> i.head) 
      }
    }
    _item2TID.toInterator
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
