package myAlg.spark.svt

// import org.apache.spark.rdd.RDD
import scala.io.Source

// start of data structure
class VertItem(TID: Int, item: String) {
  def getTID = TID
  def getItem = item
}

// start of main program
class SVT(val DB: Array[String], val minSup: Double) {
  val _rminSup = minSup * DB.length
  var results = List[String]()

  def run() {
    val _freqItems = genFreqItems(DB, _rminSup)
    val _freqItemsets = genFreqItemsets(_freqItemsets, _rminSup)
  }

  def genFreqItems(DB: Array[String], rminSup: Int): Array[VertItem]={
  }

  def genFreqItemsets(freqitems: Array[VertItem], rminSup: Int): Array[VertItem]={
  }

  def addToResults(items: Array[Vertitem]) {
  }

  def showResults() {
    results.foreach(println)
  }
}

// start of test
object MyTest extends App {
  val DB = Source.fromFile(args(0)).getLines.toArray
  val minSup = args(1).toDouble
  val model = new SVT(DB, minSup)
  model.run()
}
