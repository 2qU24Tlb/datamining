import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class Item(val TIDs: Array[Long], val itemsets: Set[String]) extends Serializable {
  // mixset type, 0 for tidset and 1 for diffset
  var ItemTyep: Int = 0

  def this(TID: Long, itemset: String) = this(Array(TID), Set(itemset))

  def +(nextItem: Item): Item = {
    new Item(this.TIDs ++ nextItem.TIDs, this.itemsets)
  }

  def sup(): Int = {
    return TIDs.length
  }

  def optimze() {
    //switch between tidset and diffset
  }

  override def toString(): String =
    "(" + this.itemsets.mkString + ":" + this.TIDs.mkString + ")"
}

object Peclat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Peclat")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:/tmp/sampledb", 2)
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val minSup = 0.5 // user defined min support
    val minSupCount = math.ceil(transactions.count * minSup).toLong
  }

  // convert transaction string to Item
  def toItem(trans: Array[String], frequents: Array[String]): Array[Item] = {
    val TID = trans(0).toLong
    val items = trans.drop(1)

    val itemArray = for {item <- items
                         if frequents.indexOf(item) > -1} yield (new Item(TID, item))
    return itemArray
  }

  // get frequent items with their mixset
  def mrCountingItems(transactions: RDD[Array[String]], minSupCount: Long): RDD[Item] = {
    val frequents = transactions.flatMap(trans => trans.drop(1).
                                           map(item => (item, 1L))).reduceByKey(_+_).
      filter(_._2 >= minSupCount).map(_._1).collect()

    val f1_items = transactions.flatMap(trans => toItem(trans, frequents).
                                          map(item => (item.itemsets.mkString, item))).reduceByKey(_+_).
      filter(_._2.sup >= minSupCount).map(_._2).cache

    f1_items
  }
  //def mrLargeK
  //def mrMiningSubtree
}
