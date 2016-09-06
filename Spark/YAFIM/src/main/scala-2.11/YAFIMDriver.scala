import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class YAFIMDriver(val minSup: Int) extends Serializable {
  var results = ArrayBuffer[Itemset]()

  def run (data: RDD[Array[String]]) {
    println("number of transactions is: " + data.count())

    // Phase I
    val f1_items = genFreqSingletons(data)
    for (i <- f1_items)
      results.append(i)

    // Phase II
    var candidates = gen2Itemsets(f1_items)
    var fk_items = Array[Itemset]()

    // Phase III
    while (candidates.length > 0) {
      fk_items = data.flatMap(x => scanDB(candidates, x)).
        map(y => (y.mkString, new Itemset(y))).
        reduceByKey(_+_).
        collect.
        filter(_._2.support >= minSup).
        map(_._2)

      scala.util.Sorting.quickSort(fk_items)

      for (i <-fk_items)
        results.append(i)

      if (fk_items.length > 0) {
        candidates = genKItemsets(fk_items)
      } else {
        candidates = Array[Array[Int]]()
      }
    }

  }

  def show() {
    for (i <- results)
        println(i)

    println("number of frequent itemsets: " + results.length)
  }

  def genFreqSingletons (transactions: RDD[Array[String]]): Array[Itemset] = {
    val f1_items = transactions.flatMap(_.map(i => (i.toInt, 1))).
      reduceByKey(_+_).
      collect.
      filter(_._2 >= minSup).
      sortBy(_._1).
      map(x => new Itemset(x._1, x._2))

    f1_items
  }

  def gen2Itemsets (freqItems: Array[Itemset]): Array[Array[Int]] = {
    val results = for (i <- 0 until freqItems.length; j <- i+1 until freqItems.length)
      yield freqItems(i).items ++ freqItems(j).items

    results.toArray
  }

  def genKItemsets (freqItems: Array[Itemset]): Array[Array[Int]] = {
    var len = 0

    if (freqItems.length >= 0) {
      len = freqItems(0).items.length
    }

    val results = for (i <- 0 until freqItems.length; j <- i+1 until freqItems.length
                       if freqItems(i).items.take(len - 1).deep == freqItems(j).items.take(len - 1).deep)
      yield freqItems(i).items ++ Array(freqItems(j).items.last)

//    for (i <- results.filter(x => checkFrequent(x, freqItems))) {
//      println(i.deep.mkString(","))
//    }

    results.filter(x => checkFrequent(x, freqItems)).toArray
  }

  //  check the candidate if all the subsets are frequent
  def checkFrequent(candidate: Array[Int], level: Array[Itemset]): Boolean = {
    var found = false

    for (pos <- candidate.indices) {
      var first: Int = 0
      var last: Int = level.length - 1

      var sample = candidate.take(pos) ++ candidate.drop(pos + 1)
      if (candidate.length == 1)
        sample = candidate

      found = false

      while (first <= last) {
        val mid: Int = (first + last) >> 1
        val comp = compareArray(sample, level(mid).items)
        if (comp == -1) {
          last = mid - 1
        } else if (comp == 1) {
          first = mid + 1
        } else {
          found = true
          first = last + 1
        }
      }

      if (! found) {
        return false
      }
    }

    true
  }

  // compare the content between two array
  // return 1 if the first if bigger, -1 if the second is bigger
  // return 0 if they are the same
  def compareArray(array1: Array[Int], array2: Array[Int]): Int = {
    for (i <- array1.indices) {
      if (array1(i) > array2(i))
        return 1
      else if (array1(i) < array2(i))
        return -1
    }
    0
  }

  // scan one transaction and find which candidates in it
  def scanDB(candidates: Array[Array[Int]], transaction: Array[String]): Array[Array[Int]] = {
    var results = ArrayBuffer[Array[Int]]()

    for (i <- candidates) {
      var pos: Int = 0
      for (j <- transaction if pos < i.length) {
        if (j.toInt == i(pos)) {
          pos += 1
          if (pos == i.length) {
            results += i
          }
        } else if (j.toInt > i(pos))
          pos = i.length
      }
    }

    results.toArray
  }

}
