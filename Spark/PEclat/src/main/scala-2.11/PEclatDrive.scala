import java.{util => ju}

import PEclatDrive.FreqItems
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PEclatDrive extends Serializable {
  var results: ListBuffer[FreqItems] = ListBuffer()

  def run(transactions: RDD[Array[String]], minSup: Double) {
    val partitioner = new HashPartitioner(transactions.partitions.length)

    // Phase I: Find frequent singletons
     val f1_items = transactions.
       flatMap(_.drop(1).map(i => (i, 1))).
       reduceByKey(_+_).
       collect().
       filter(_._2 >= minSup).
       sortBy(_._2)

    for (i <- f1_items) {
      results.append(new FreqItems(Array(i._1), Array(i._2)))
    }

    val f1Items = f1_items.map(_._1)

    // Phase II: Get Equivalent Class
    val rankItems = f1Items.zipWithIndex.toMap
    println(rankItems)
    val revRankItems = rankItems.map(_.swap)

    val freqItems = transactions.flatMap{trans => genEQClass(trans, rankItems, partitioner)}.
      aggregateByKey(new EQClass, partitioner.numPartitions)(
        (EQCList, candidates) => EQCList.add(candidates),
        (EQCList1, EQList2) => EQCList1.merge(EQList2)).
       // Phase III: Subtree Mining
      flatMap { case (part, why) => why.mine(minSup, x => partitioner.getPartition(x) == part)}.
      map{case (ranks, count) => new FreqItems(ranks.map(i => revRankItems(i)), count)}

    for (i <- freqItems.collect) {
      results.append(i)
    }
  }

  def show(): Unit = {
    println("frequent items:")
    results.foreach{ itemset => println(itemset.nodes.sorted.mkString("[",",","]")
      + ":"
      + itemset.TIDs.mkString(","))}
  }

  // scan DB and get EQ class
  // return : target partition -> (prefix, suffix, TID)
  def genEQClass(data: Array[String],
                 rankItems: Map[String, Int],
                 partitioner: Partitioner): mutable.Map[Int, ListBuffer[(Int, Array[Int], Int)]] = {

    val output = mutable.Map.empty[Int, mutable.ListBuffer[(Int, Array[Int], Int)]]
    val TID = data.head.toInt

    val filtered = data.drop(1).flatMap(rankItems.get)
    ju.Arrays.sort(filtered)

    for (i <- 0 until filtered.length - 1) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)

      val candidates = output.getOrElseUpdate(part, ListBuffer[(Int, Array[Int], Int)]())
      candidates.append((filtered(i), filtered.slice(i+1, filtered.length), TID))
    }

    output
  }

}

object PEclatDrive extends Serializable {
  class FreqItems(val nodes:Array[String], val TIDs:Array[Int]) extends Serializable {}
}
