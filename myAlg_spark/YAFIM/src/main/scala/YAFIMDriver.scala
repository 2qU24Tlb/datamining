package unioah.spark.fpm

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.control._
import scala.collection.mutable.ArrayBuffer

class YAFIM(val minSup: Int) extends Serializable {
  var results = Array[Itemset]()

  def run (data: RDD[Array[String]]) {
    val f1_items = genFreqSingletons(data)
    println("number of frequent singletons is: " + f1_items.size)
    for (i <- f1_items)
        println(i)

    results = genFreItemsets(data, f1_items)
  }

  def show() {
    for (i <- results)
        println(i)
  }

  // Phase I
  def genFreqSingletons (transactions: RDD[Array[String]]): Array[Itemset] = {
    val f1_items = transactions.flatMap(_.map(i => (i.toInt, 1))).
      reduceByKey(_+_).
      filter(_._2 >= minSup).
      sortBy(_._1).
      map(x => new Itemset(x._1, x._2)).
      collect

    return f1_items
  }

  // Phase II
  def genFreItemsets (transactions: RDD[Array[String]], freqItems: Array[Itemset]):
      Array[Itemset] = {
    var level = freqItems
    var freqItemsets = freqItems

    while (level.length != 0) {
      var candidates = genCandidates(level)
      var current = transactions.flatMap(x => scanDB(candidates, x))
        .map(x => (x.mkString, new Itemset(x)))
        .reduceByKey(_+_)
        .filter(_._2.sup >= minSup)
        .sortBy(_._1)
        .map(_._2).collect

      for (i <- current)
        println(i)

      level = current

      if (level.length != 0)
        freqItemsets = level
    }

    return freqItemsets
  }

  // generate candidates for Phase II
  def genCandidates(prevLevel: Array[Itemset]): Array[Array[Int]] = {
    var candidate = Array[Int]()
    var candidates = ArrayBuffer[Array[Int]]()
    val length = prevLevel.length

    val outer = new Breaks

    for (i <- 0 to length - 1) {
      outer.breakable {
        for (j <- i + 1 to length - 1) {
          val item1 = prevLevel(i).itemset
          val item2 = prevLevel(j).itemset

          for (k <- 0 to item1.length - 1) {
            if (k == item1.length - 1) {
              if (item1(k) > item2(k))
                outer.break
            } else if (item1(k) != item2(k)) {
                outer.break
            }
          }
          candidate = item1 ++ Array(item2.last)

          if (checkFrequent(candidate, prevLevel)) {
            candidates += candidate
          }
        }
      }
    }

    return candidates.toArray
  }

  //  check the candidate if all the subsets are frequent
  def checkFrequent(candidate: Array[Int], level: Array[Itemset]): Boolean = {
    var found = false

    for (pos <- 0 to candidate.length - 1) {
      var first: Int = 0
      var last: Int = level.length - 1

      var sample = candidate.take(pos) ++ candidate.drop(pos + 1)
      if (candidate.length == 1)
        sample = candidate

      found = false

      val outer = new Breaks
      outer.breakable {
        while (first <= last) {
          var mid: Int = (first + last) >> 1
          var comp = compareArray(sample, level(mid).itemset)
          if (comp == -1) {
            last = mid - 1
          } else if (comp == 1) {
            first = mid + 1
          } else {
            found = true
            outer.break
          }
        }
      }
      if (found == false) {
        return false
      }
    }

    return true
  }

  // compare the content between two array
  // return 1 if the first if bigger, -1 if the second is bigger
  // return 0 if they are the same
  def compareArray(array1: Array[Int], array2: Array[Int]): Int = {
    for (i <- 0 to array1.length - 1) {
      if (array1(i) > array2(i))
        return 1
      else if (array1(i) < array2(i))
        return -1
    }
    return 0
  }

  // scan database to get the support for the itemsets
  def scanDB(candidates: Array[Array[Int]], transaction: Array[String]): Array[Array[Int]] = {
    var results = ArrayBuffer[Array[Int]]()

    for (i <- candidates) {
      var pos: Int = 0
        for (j <- transaction) {
          if (pos < i.length) {
            if (j.toInt == i(pos)) {
              pos += 1
              if (pos == i.length) {
                results += i
              }
            } else if (j.toInt > i(pos))
              pos = i.length
          }
      }
    }

    return results.toArray
  }

}
