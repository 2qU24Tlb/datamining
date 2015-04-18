package myAlg.spark.wordcount

//import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    //val sc = new SparkContext(new SparkConf().setAppName("Count Word"))
    // val threshold = args(1).toInt
    
    // split each document into words
    // val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))
    
    // count the occurrence of each word
    // val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
    
    // filter out words with less than threshold occurrences
    // val filtered = wordCounts.filter(_._2 >= threshold)
    
    // System.out.println(wordCounts.collect().mkString(", "))

    val point1 = new Point(1, 1)
    val point2 = new ColorPoint(1, 1, "red")

    point1.printPoint()
    point2.printPoint()
  }

  class Point(xc: Int, yc: Int) {
    var x: Int = xc
    var y: Int = yc

    def move(dx: Int, dy: Int) {
      x = x + dx
      y = y + dy
    }

    def printPoint() {
      println(x + "," + y)
    }
  }

  class ColorPoint(u: Int, v: Int, c: String) extends Point(u, v) {
    val color: String = c

    override def printPoint() {
      println(x + "," + y + ":" + c)
    }
  }
}
