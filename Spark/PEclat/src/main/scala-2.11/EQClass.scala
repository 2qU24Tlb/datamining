import EQClass._

import scala.collection.mutable.{Map => mMap}
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

class EQClass extends Serializable{
  val EQCTree: mMap[Int, ListBuffer[EQList]] = mMap.empty
  val freqItems = ArrayBuffer[(Array[Int], Array[Int])]()

  // add a new set of equivalent class to this list
  def add(candidates: ListBuffer[(Int, Array[Int], Int)]): this.type = {
    candidates.foreach {
      pair =>
        val prefix = pair._1
        val curPair = EQCTree.getOrElseUpdate(prefix, {
          val newEQList = new EQList(Array(prefix))
          val newList = ListBuffer(newEQList)
          newList
        }).head

        for (i <- pair._2)
          if (!curPair.suffixs.contains(i))
            curPair.suffixs(i) = Set(pair._3)
          else
            curPair.suffixs(i) = curPair.suffixs(i) | Set(pair._3)
    }

    this
  }

  // merge two EQClass in the same partition
  def merge(other: EQClass): this.type = {

    for (i <- other.EQCTree.keys) {
      if (!this.EQCTree.contains(i)) {
        this.EQCTree(i) = other.EQCTree(i)
      } else {
        val curPair = this.EQCTree(i).head
        val otherPair = other.EQCTree(i).head

        for (j <- otherPair.suffixs.keys) {
          if (!curPair.suffixs.contains(j)) {
            curPair.suffixs(j) = otherPair.suffixs(j)
          } else {
            curPair.suffixs(j) = curPair.suffixs(j) | otherPair.suffixs(j)
          }
        }
      }
    }

    this
  }

  // mine EQClass and generate frequent items
  def mine(minSup:Double, checkPrefix: Int => Boolean = _ => true):
  Array[(Array[Int], Array[Int])] = {

    for ((prefix, curList) <- EQCTree) {
      if (checkPrefix(prefix)) {
        for (curEQ <- curList)
          mineEQ(curEQ, minSup)
          //testMine(curEQ, minSup)
      }
    }

    freqItems.toArray
  }

  def testMine(node:EQList, minSup: Double): Unit = {
    val curSuffix = node.suffixs.keys.toArray.sorted

    for (i <- 0 until curSuffix.length) {
      val itemsets = node.prefix ++ Array(curSuffix(i))
      val TID = node.suffixs(curSuffix(i))

      freqItems.append((itemsets, TID.toArray.sorted))
    }
  }

  // mine a equivalent class
  def mineEQ(node: EQList, minSup: Double): Unit = {
    val curSuffixs = node.suffixs.keys.toArray.sorted

    // if there is only one suffix in equivalent class
    if (curSuffixs.length == 1) {
      val itemsets = node.prefix ++ Array(curSuffixs.head)
      val TIDs = node.suffixs(curSuffixs.head)
      if (TIDs.size >= minSup) {
        freqItems.append((itemsets, TIDs.toArray.sorted))
      }

      return
    }

    for (i <- 0 until curSuffixs.length) {
      val candidates: mMap[Int, Set[Int]] = mMap.empty
      val TIDI = node.suffixs(curSuffixs(i))

      if (TIDI.size >= minSup) {
        val itemsets = node.prefix ++ Array(curSuffixs(i))
        freqItems.append((itemsets, TIDI.toArray.sorted))

        for (j <- i + 1 until curSuffixs.length if i != curSuffixs.length - 1) {
          val TIDJ = node.suffixs(curSuffixs(j))
          val interIJ = TIDI  & TIDJ

          if (interIJ.size >= minSup) {
            candidates(curSuffixs(j)) = interIJ
          }
        }

        if (candidates.nonEmpty) {
          val newEQC = new EQList(node.prefix ++ Array(curSuffixs(i)))
          newEQC.suffixs = candidates
          mineEQ(newEQC, minSup)
        }
      }
    }

  }

}

object EQClass {
  class EQList(val prefix: Array[Int]) extends Serializable {
    var suffixs: mMap[Int, Set[Int]] = mMap.empty
  }
}

