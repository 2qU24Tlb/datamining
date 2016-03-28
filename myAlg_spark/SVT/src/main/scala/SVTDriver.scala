import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.RangePartitioner


// start of data structure
class Item(val items: Array[String], val TIDs: Set[Long], val sup: Long) extends Serializable {
  val prefix = items.take(items.size - 1).mkString
  var tidType: Int = 1 // 1 for tidSet, 2 for diffSet

  def this(item: String, TID: Long) = this(Array(item), Set(TID), 1l)

  def +(another: Item): Item = {
    new Item(this.items, this.TIDs ++ another.TIDs, this.sup + another.sup)
  }

  def &(another: Item): Item = {
    val newTIDs = this.TIDs & another.TIDs
    val newItems = (this.items.toSet ++
                      another.items.toSet).toArray.sorted
    val newSup = newTIDs.size.toLong

    return new Item(newItems, newTIDs, newSup)
  }

  override def toString(): String = {
    //"(" + this.items.mkString(".") + ":" + this.TIDs.toList.sorted.mkString("_") + ")"
    "(" + this.items.mkString(".") + ":" + this.TIDs.size.toString() + ")"
  }
}

// start of test
object SVT {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SVT")
    val sc = new SparkContext(conf)

    Utils.debug = true

    // args(0) for transactions, args(1) for minSup
    val data = sc.textFile("file:/tmp/mushrooms_lined.txt")
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val trans_num = transactions.count
    val minSup = args(0).toDouble // user defined min support
    val minSupCount = math.ceil(minSup * trans_num).toLong
    val kCount = 3 // find all frequent k-itemsets

    println("number of transactions: " + trans_num.toString() + "\n")

    val f1_items = genFreqSingletons(transactions, minSupCount)
    //Utils.showResult("singletons: ", f1_items)

    val fk_items = genKItemsets(f1_items, kCount, minSupCount)
    //Utils.showResult("Equivalent Class: ", fk_items)

    val fre_items = paraMining(fk_items, minSupCount)
    Utils.showResult("Number of closed frequent itemsets: ", fre_items)

    sc.stop
  }

  def genFreqSingletons(transactions: RDD[Array[String]], minSupCount: Long): RDD[Item] = {
    val f1_items = transactions.flatMap(toItem(_)).
      reduceByKey(_ + _).
      filter(_._2.sup >= minSupCount).map(_._2).cache()

    return f1_items
  }

  // convert the transaction string to Item
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
          filter(_._2.sup >= minSupCount).map(_._2).cache()
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
        filter(_.sup >= minSupCount).cache()
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
