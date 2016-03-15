import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Set
import org.apache.spark.RangePartitioner
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

class Item(val items: Array[String], val tidSet: Set[Long]) extends Serializable {
  // mixset type, 0 for tidset and 1 for diffset
  var tidType: Int = 0
  var diffSet: Set[Long] = Set()

  def this(item: String, tid: Long) = this(Array(item), Set(tid))

  // increase count on the same itemset
  def +(next: Item): Item = {
    new Item(this.items, this.tidSet ++ next.tidSet)
  }

  // combine two itemsets together to get subset
  def ++(next: Item): Item = {
    val sub = new Item((this.items.toSet ++ next.items.toSet).toArray.sorted,
                       this.tidSet & next.tidSet)

    sub.diffSet = this.tidSet -- next.tidSet

    if (sub.diffSet.size < sub.tidSet.size)
      sub.tidType = 1

    return sub
  }

  def prefix(): Set[String] = items.take(items.size - 1).toSet

  def sup(): Int = {
    return tidSet.size
  }

  // choose between tidset and diffset for a better performance
  // just for first level ?
  def optimize(allTrans: Set[Long]) {
    this.diffSet = allTrans -- tidSet

    if (this.diffSet.size < this.tidSet.size)
      this.tidType = 1
  }

  override def toString(): String = {
    "(" + this.items.toList.sorted.mkString + ":" + this.tidSet.toList.sorted + ")"
  }
}

object Peclat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Peclat")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:/tmp/BMS1_itemset_mining_numed.txt")
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val minSup = 0.002 // user defined min support
    val minSupCount = math.ceil(transactions.count * minSup).toLong
    val kCount = 3 // find all frequent k-itemsets

    val f1_items = mrCountingItems(transactions, minSupCount)
    println("Stage 1 completed! " + f1_items.count().toString() + " number of frequent items")

    val fk_items = mrLargeK(f1_items, kCount, minSupCount)
    println("Stage 2 completed! " + fk_items.count().toString() + " number of frequent items")
    // val tmp = fk_items.collect()
    // for (x <- tmp) {
    //   println(x)
    // }

    val results = mrMiningSubtree(fk_items, minSupCount)
    println("Stage 3 completed! " + results.count().toString() + " number of frequent items")
  }

  // get frequent items with their mixset
  def mrCountingItems(transactions: RDD[Array[String]], minSupCount: Long): RDD[Item] = {
    // frequent 1-item set
    val frequents = transactions.flatMap(trans => trans.drop(1).map(item => (item, 1L))).
      reduceByKey(_ + _).filter(_._2 >= minSupCount).map(_._1).collect()

    // frequent items
    val f1_items = transactions.flatMap(trans => toItem(trans, frequents).
                                          map(item => (item.items.mkString, item))).
      reduceByKey(_ + _).filter(_._2.sup >= minSupCount).map(_._2).cache

    // frequent mixset
    val allTIDs = transactions.map(trans => trans(0).toLong).collect().toSet
    f1_items.map(_.optimize(allTIDs))

    return f1_items
  }

  // convert the transaction strings to Item
  def toItem(trans: Array[String], frequents: Array[String]): Array[Item] = {
    val TID = trans(0).toLong
    val itemString = trans.drop(1)

    val itemArray = for {
      item <- itemString
      if frequents.indexOf(item) > -1
    } yield (new Item(item, TID))

    return itemArray
  }

  // get frequent k-itemsets
  def mrLargeK(freItems: RDD[Item], kCount: Int, minSupCount: Long): RDD[Item] = {
    var freSubSet = freItems
    var preFreSubSet = freSubSet
    var length = 1 //cardinality of frequent itemset
    var myCount = kCount

    while (myCount > 0) {
      if (!freSubSet.isEmpty()) {
        val tmp = freSubSet.map(_.items.toSet).cache()

        // [FixMe] maybe very slow for a large number to do Cartesian. eg. 100^100
        val candidates = tmp.cartesian(tmp).map(item => item._1 ++ item._2).map((_,1)).
          reduceByKey(_+_).
          filter(item => (item._1.size == (length + 1)) && (item._2 == (length + 1) * length)).
          map(_._1).collect

        preFreSubSet = freSubSet
        freSubSet = freSubSet.flatMap(genSuper(candidates, _)).
          reduceByKey(_ ++ _).map(_._2).filter(_.sup >= minSupCount)
      }
      myCount -= 1
      length += 1
    }
    if (freSubSet.isEmpty()) {
      freSubSet = preFreSubSet
    }

    return freSubSet
  }

  // generate all the super set for this item
  def genSuper(candidates: Array[Set[String]], item: Item): Array[(Set[String], Item)] = {
    val itemSet = item.items.toSet
    val superSet = candidates.filter(itemSet subsetOf(_)).map((_, item))
    return superSet
  }

  def mrMiningSubtree(freKItems: RDD[Item], minSupCount: Long): RDD[Item] = {
    val EQClass = freKItems.keyBy(_.prefix().toList.sorted.mkString)
    val EQClassCount = freKItems.map(item => (item.prefix(), 1)).reduceByKey(_+_).count().toInt

    //re-partition
    var freSubSet = EQClass.partitionBy(new RangePartitioner(EQClassCount, EQClass)).map(_._2).persist()
    freSubSet = freSubSet.mapPartitions(mergerSame, true)
    var preFreSubSet = freSubSet

    while(!freSubSet.isEmpty()) {
      preFreSubSet = freSubSet
      freSubSet = freSubSet.mapPartitions(localMining)
    }
    freSubSet = preFreSubSet

    return freSubSet
  }

  // merge items with same itemsets after re-shuffle
  def mergerSame(iter: Iterator[Item]): Iterator[Item] = {
    var itemList = Map[String, Item]()

    while(iter.hasNext) {
      var cur = iter.next
      var itemKey = cur.items.toList.sorted.mkString
      if (itemList.contains(itemKey)) {
        itemList(itemKey) + cur
      } else {
        itemList += (itemKey -> cur)
      }
    }

    return itemList.values.iterator
  }

  // local E/D-clat mining
  def localMining(iter: Iterator[Item]): Iterator[Item] = {
    var itemMap = Map[String, ArrayBuffer[Item]]()
    val results = ArrayBuffer.empty[Item]

    while(iter.hasNext) {
      var cur = iter.next
      var itemKey = cur.prefix().toList.sorted.mkString
      if (itemMap.contains(itemKey)) {
        for (prev <- itemMap(itemKey)) {
          results += (prev ++ cur)
        }
        itemMap(itemKey) +=  cur
      } else {
        itemMap += (itemKey -> ArrayBuffer(cur))
      }
    }

    return results.iterator
  }

}
