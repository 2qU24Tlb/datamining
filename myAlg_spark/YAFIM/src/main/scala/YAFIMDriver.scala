package unioah.spark.fpm

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.control._
//import org.apache.jorphan.collections
import unioah.spark.fpm.Itemset

class YAFIM(val minSup: Int) extends Serializable {
  var results = Array[Itemset]()

  def run[Item: ClassTag] (data: RDD[Array[Item]]) {
    f1_items = genFreqSingletons(data, minSup)

    return f1_items
  }

  def show() {
  }

  // Phase I
  def genFreqSingletons[data: ClassTag](transactions: RDD[Array[data]]): Array[Itemset] = {
    val f1_items = transactions.flatMap(_.map(i => (i.toInt, 1L))).
      reduceByKey(_+_).
      filter(_._2 >= minSup).
      sortBy(_._1).
      map(x => new Itemset(x._1)).
      collect

    return f1_items
  }

  // Phase II
  def genFreItemsets[data: ClassTag](transactions: RDD[Array[data]], freqItem: Array[Itemset]): Array[Itemset] = {
    var level = freqItem
    var k = 2

    while (level.length != 0) {
    }
  }

  // generate candidates for Phase II
  def genCandidates(prevLevel: Array[Itemset]): Array[Itemset] = {
    var candidate = Array[Int]()
    var candidates = Array[Itemset]()
    val length = prevLevel.length

    val outer = new Breaks
    val inner = new Breaks

    for (i <- 0 to length - 1) {
      outer.breakable {
        for (j <- i + 1 to length - 1) {
          val item1 = prevLevel(i).itemset
          val item2 = prevLevel(j).itemset

          for (k <- 0 to item1.length - 1) {
            if (k == item1.length - 1) {
              if (item1[k] > item2[k])
                outer.break
            } else if (item1[k] != item2[k]) {
                outer.break
            }
          }
          candidate = item1 ++ item2.last

          if (checkSupport(candidate.itemset, prevLevel)) {
            candidates += candidate
          }
        }
      }
    }

    return candidates
  }

  //  check the candidate if all the subsets are frequent
  def checkSupport(candidate: Array[Int], level: Array[Itemset]): Boolean = {
    for (pos <- 0 to level.length - 1) {
      var first: Int = 0
      var last: Int = level.length - 1
      var sample = candidate.drop(pos)

      while (first <= last) {
        var mid: Int = (first + last) >> 1
        var comp = compareArray(sample, level[mid].itemset)
        if (comp == -1) {
          first = mid + 1
        } else if (comp == 1) {
          last = mid - 1
        } else {
          return true
        }
      }
    }

    return false
  }

  // compare the content between two array
  // return 1 if the first if bigger, -1 if the second is bigger
  // return 0 if they are the same
  def compareArray(array1: array[Int], array2: array[Int]): Int = {
    for (i <- 0 to array1.length - 1) {
      if (array1[i] > array2[i])
        return 1
      else if (array1[i] < array2[i])
        return -1
    }

    return 0
  }
}
