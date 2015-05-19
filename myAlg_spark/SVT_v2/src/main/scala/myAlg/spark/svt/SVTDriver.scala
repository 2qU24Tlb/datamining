package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

// start of data structure
class VertItem(TID: Int, item: String) {
  def getTID = TID
  def getItem = item
}

// start of main program
class SVTDriver(val DB: RDD[Array[String]], val minSup: Double) {
  val _dbLength = DB.count
  val _rminSup = (minSup * _dbLength).ceil.toInt
  var results = List[String]()

  def run() {
    println("DB Length: " + _dbLength.toString)
    //val _freqItems = genFreqItems(DB, _rminSup)
    //val _freqItemsets = genFreqItemsets(_freqItems, _rminSup)
  }

  def genFreqItems(DB: Array[String], rminSup: Int): Array[VertItem]={
    var tmp = new Array[VertItem](1)
    tmp
  }

  def genFreqItemsets(freqitems: Array[VertItem], rminSup: Int): Array[VertItem]={
    var tmp = new Array[VertItem](1)
    tmp
  }

  def addToResults(items: Array[VertItem]) {
  }

  def showResults() {
    results.foreach(println)
  }
}

// start of test
object MyTest extends App {
  val conf = new SparkConf().setAppName("Scale Vertical Mining example")
  val sc = new SparkContext(conf)

  val DB = sc.textFile(args(0)).map(_.split(" ")).cache()
  val minSup = args(1).toDouble


  val model = new SVTDriver(DB, minSup)
  model.run()
}
