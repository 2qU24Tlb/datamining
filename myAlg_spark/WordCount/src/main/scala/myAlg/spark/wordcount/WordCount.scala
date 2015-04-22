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

    val a = List(("12", "12"), ("13", "13"), ("14", "23"))

  }

  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[T]()
    var pre = iter.next
    while (iter.hasNext)
    {
      val cur = iter.next;
      res .::= (pre + cur)
      pre = cur;
    }
    res.iterator
  }

}
