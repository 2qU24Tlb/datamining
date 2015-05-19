package myAlg.spark.svt

// import org.apache.spark.rdd.RDD
import scala.io.Source

class VertItem(TID: Int, Item: String) {
  val _TID = TID
  val _Item = Item
}

class SVT(val DB: Array[String], val minSup: Double) {
  val _minSup = minSup
  val _rminSup = minSup * DB.size

  def run() {
    println(minSup)
  }
}

// start of test
object MyTest extends App {
  val DB = Source.fromFile(args(0)).getLines.toArray
  val minSup = args(1).toDouble
  val model = new SVT(DB, minSup)
  model.run()
}
