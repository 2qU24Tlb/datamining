import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Set

class Item(val items: Array[String], val tidSet: Set[Long], val diffSet: Set[Long]) extends Serializable {
  // mixset type, 0 for tidset and 1 for diffset
  var tidType: Int = if (tidSet.size < diffSet.size) 0 else 1

  def this(item: String, tid: Long, allTIDs: Set[Long]) = this(Array(item), Set(tid), allTIDs - tid)

  // increase count on the same itemset
  def +(next: Item): Item = {
    new Item(this.items, this.tidSet ++ next.tidSet, this.diffSet & next.diffSet)
  }

  // Intersection of two items
  def &(next: Item): Item = {
    new Item(this.items, this.tidSet & next.tidSet, this.diffSet & next.diffSet)
  }

  // combine two itemsets together to get subset
  def ++(next: Item): Item = {
    new Item((this.items.toSet ++ next.items.toSet).toArray.sorted,
                       this.tidSet & next.tidSet, this.diffSet & next.diffSet)
  }

  def prefix(): String = items.take(items.size - 1).mkString

  def sup(): Int = {
    return tidSet.size
  }

  override def toString(): String = {
    "(" + this.items.toList.sorted.mkString + ":" + this.tidSet.toList.sorted + ")"
  }
}

object Peclat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Peclat")
    val sc = new SparkContext(conf)

    //val data = sc.textFile("file:/tmp/sampledb", 2)
    val data = sc.textFile("file:/tmp/BMS1_itemset_mining_numed.txt", 8)
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val minSup = 0.5 // user defined min support
    val minSupCount = math.ceil(transactions.count * minSup).toLong
    val kCount = 3 // find all frequent k-itemsets

    val f1_items = mrCountingItems(transactions, minSupCount).cache()
    println("Stage 1 completed! ")

    val fk_items = mrLargeK(f1_items, kCount, minSupCount).cache()
     println("Stage 2 completed! " + fk_items.count().toString() + " number of frequent items")

    val results = mrMiningSubtree(fk_items, minSupCount)
    println("Stage 3 completed! " + results.count().toString() + " number of frequent items")
  }

  // get frequent items with their mixset
  def mrCountingItems(transactions: RDD[Array[String]], minSupCount: Long): RDD[Item] = {
    // all TIDs
    val allTIDs = transactions.map(trans => trans(0).toLong).collect().toSet
 
    // frequent items
    val f1_items = transactions.flatMap(toItem(_, allTIDs).map(item => (item.items.mkString, item))).
      reduceByKey(_ + _).filter(_._2.sup >= minSupCount).map(_._2)

    return f1_items
  }

  // convert the transaction strings to Item
  def toItem(trans: Array[String], allTIDs: Set[Long]): Array[Item] = {
    val TID = trans(0).toLong
    val itemString = trans.drop(1)

    val itemArray = for {
      item <- itemString
    } yield (new Item(item, TID, allTIDs))

    return itemArray
  }

  // get frequent k-itemsets
  def mrLargeK(freItems: RDD[Item], kCount: Int, minSupCount: Long): RDD[Item] = {
    var freSubSet = freItems
    var preFreSubSet = freSubSet
    var myCount = kCount

    while (myCount > 0) {
      if (!freSubSet.isEmpty()) {
        preFreSubSet = freSubSet

        val preFreSet = freSubSet.map(_.items).collect()
        val candidates = genKCandidets(preFreSet)

        freSubSet = freSubSet.flatMap(genSubSets(_, candidates)).
          map(item => (item.items.mkString, item)).reduceByKey(_&_).
          filter(_._2.sup >= minSupCount).map(_._2)
      }

      myCount -= 1
    }

    if (freSubSet.isEmpty()) {
      freSubSet = preFreSubSet
    }

    return freSubSet
  }

  def genKCandidets(superSet: Array[Array[String]]): Array[Array[String]] = {
    val k = superSet(0).length + 1
    var result = ArrayBuffer[Array[String]]()

    if (k == 2) {

      for (i <- 0 to superSet.size - 1; j <- i+1 to superSet.size - 1)
        result += (superSet(i) ++ superSet(j)).sorted

    } else {

      var itemMap = Map[String, ArrayBuffer[Array[String]]]()
      for ( i <- superSet) {

        val prefix = i.take(k-2).mkString
        if (itemMap.contains(prefix))
          for (j <- itemMap(prefix))
            result += (i.toSet ++ j.toSet).toArray.sorted
        else
          itemMap += (prefix -> ArrayBuffer(i))
      }
    }

    return result.toArray
  }

  // generate all the super set for this item
  def genSubSets(curItem: Item, candidates: Array[Array[String]]): Array[Item] = {
    var equivClass = Array[Array[String]]()

    if (curItem.items.length == 1)
      equivClass = candidates.filter(_.contains(curItem.items.last))
    else {
      val prefix = curItem.prefix()
      equivClass = candidates.filter(_.mkString.startsWith(prefix)).
        filter(_.contains(curItem.items.last))
    }

    val result = for (item <- equivClass) yield (new Item(item, curItem.tidSet, curItem.diffSet))

    return result
  }

  def mrMiningSubtree(freKItems: RDD[Item], minSupCount: Long): RDD[Item] = {
    val EQClass = freKItems.keyBy(_.prefix())
    val EQClassCount = freKItems.map(item => (item.prefix(), 1)).reduceByKey(_+_).count().toInt

    //re-partition
    var freSubSet = EQClass.partitionBy(new RangePartitioner(EQClassCount, EQClass)).map(_._2).persist()
    freSubSet = freSubSet.mapPartitions(mergerSame, true)
    var preFreSubSet = freSubSet

    while(!freSubSet.isEmpty()) {
      preFreSubSet = freSubSet
      freSubSet = freSubSet.mapPartitions(localMining).filter(_.sup() >= minSupCount).cache()
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
      var itemKey = cur.prefix()
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
