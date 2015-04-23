package myAlg.spark.svt

import scala.collection._
import scala.collection.mutable.ListBuffer


// Horizontal transactions
class HSets(transactions: String) {
  val transList = transactions.split(" ")
  val itemSets = transactions.tail
  def tid: String = transList(0)
  def findFSets(GFSets: Map[String, Int], minSup: Int): List[String] = {
    val LFSets = ListBuffer.empty[String]

    for (x <- itemSets) {
      if (GFSets(x.toString) >= minSup) {
        LFSets += x.toString
      }
    }
    LFSets.sorted.toList
  }
}
