package unioah.spark.fpm

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.reflect.ClassTag
import org.apache.spark.RangePartitioner


class SVT (val minSupport: Double) extends Serializable {
  def run[Item: ClassTag](data: RDD[Array[Item]]) {
    val minSupCount = math.ceil(minSupport * data.count).toLong
    val freqItems = genFreqSingletons(data, minSupCount)
    println(freqItems.size)
  }

  def genFreqSingletons[data: ClassTag](transactions: RDD[Array[data]], minSupCount: Long): Array[data] = {
    val f1_items = transactions.flatMap{_.drop(1)}
      .map(i => (i, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupCount)
      .collect
      .sortBy(_._2)
      .map(_._1)

    return f1_items
  }

  def genKItemsets[data: ClassTag](data: RDD[Array[data]], minCount: Long, freqItems: Array[data]):
      RDD[SVTItem] = {
  }
}

  // convert the transaction string to Item
/*
  def toItem(trans: Array[String]): Array[(String, Item)] = {
    val TID = trans(0).toLong
    val itemString = trans.drop(1)

    val itemArray = for {
      item <- itemString
    } yield {(item, new Item(item, TID))}

    return itemArray
  }

  def genKItemsets(freItems: RDD[Item], kCount: Int, minSupCount: Long): RDD[Item] = {
    var freSubSet = freItems
    var preFreSubSet = freSubSet
    var myCount = kCount

    while (myCount > 0) {
      if (!freSubSet.isEmpty()) {
        preFreSubSet = freSubSet

        val preFreSet = freSubSet.map(_.items).collect()
        val candidates = genKCandidets(preFreSet)

        freSubSet = freSubSet.flatMap(genSubSets(_, candidates)).
          reduceByKey(_&_).
          filter(_._2.sup >= minSupCount).map(_._2)
      }

      myCount -= 1
    }

    if (freSubSet.isEmpty()) {
      freSubSet = preFreSubSet
    }

    return freSubSet
  }

  // generate all the candidates from previous frequent items
  def genKCandidets(superSet: Array[Array[String]]): Array[Array[String]] = {
    val k = superSet(0).length + 1
    var result = ArrayBuffer[Array[String]]()

    if (k == 2) {
      for (i <- 0 to superSet.size - 1;
           j <- i+1 to superSet.size - 1)
        result += (superSet(i) ++ superSet(j)).sorted
    } else {
      var itemMap = Map[String, ArrayBuffer[Array[String]]]()
      for ( i <- superSet) {
        val prefix = i.take(k-2).mkString
        if (itemMap.contains(prefix)) {
          for (j <- itemMap(prefix)) {
            result += (i.toSet ++ j.toSet).toArray.sorted
          }
          itemMap(prefix) += i
        }
        else
          itemMap += (prefix -> ArrayBuffer(i))
      }
    }

    return result.toArray
  }

  // link items with their corresponding candidates
  def genSubSets(curItem: Item, candidates: Array[Array[String]]): Array[(String, Item)] = {
    var equivClass = Array[Array[String]]()

    if (curItem.items.length == 1)
      equivClass = candidates.filter(_.contains(curItem.items.last))
    else {
      val prefix = curItem.prefix
      equivClass = candidates.filter(_.mkString.startsWith(prefix)).
        filter(_.contains(curItem.items.last))
    }

    val result = for (item <- equivClass) yield {(item.mkString, curItem)}

    return result
  }

  // parallel vertical mining
  def paraMining(freKitems: RDD[Item], minSupCount: Long): RDD[Item] = {
    val EQClass = freKitems.keyBy(_.prefix)
    val EQClassCount = freKitems.map(item => (item.prefix, 1)).
      reduceByKey(_+_).count().toInt

    //re-partition
    var freSubSet = EQClass.partitionBy(
      new RangePartitioner(EQClassCount, EQClass)).
      map(_._2).persist()

    var preFreSubSet = freSubSet

    while(!freSubSet.isEmpty()) {
      preFreSubSet = freSubSet
      freSubSet = freSubSet.mapPartitions(localMining).
        filter(_.sup >= minSupCount)
    }
    freSubSet = preFreSubSet

    return freSubSet
  }

  // local E/D-clat mining
  def localMining(iter: Iterator[Item]): Iterator[Item] = {
    var itemMap = Map[String, ArrayBuffer[Item]]()
    val results = ArrayBuffer.empty[Item]

    while(iter.hasNext) {
      var cur = iter.next
      var itemKey = cur.prefix
      if (itemMap.contains(itemKey)) {
        for (prev <- itemMap(itemKey)) {
          results += (prev & cur)
        }
        itemMap(itemKey) +=  cur
      } else {
        itemMap += (itemKey -> ArrayBuffer(cur))
      }
    }

    return results.iterator
  }

object Utils {
  var debug = false

  // check partition status
  def checkParttion[T] (part: RDD[T]) {
    part.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)
  }

  def showResult(label: String, freKitems: RDD[Item]) {
    if (debug) {
      val results = freKitems.collect()
      println(label + results.length.toString())
      for (i <- results)
        println(i)
      println()
    }
    else
      println("done!")
  }

  //[Todo]function: write results to log file
}

 */
