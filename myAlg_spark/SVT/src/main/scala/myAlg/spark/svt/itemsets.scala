package myAlg.spark.svt

import scala.collection._
import scala.collection.mutable.ListBuffer


// Horizontal transactions
class HSets(transaction: Array[String]) {
  val transList = transaction.split(" ").cache
  def tid: String = transList(0)
  def itemSets: Array[String] = transactions.tail.cache
  def findFSets(GFSets: Map[String, Int], minSup: int): List[String] = {
    val LFSets = ListBuffer.empty[String]

    for (x <- itemSets) {
      if (GFSets(x) >= minSup) {
        LFSets += x
      }
    }
    LFSets.sorted.toList
  }
}

// Equivalent Class
class VSets() {
  //val long tid sets
  //val support
  //val item name
  //def +
  //def -
}
