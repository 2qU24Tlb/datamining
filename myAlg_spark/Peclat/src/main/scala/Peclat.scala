import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class Item(val items: Array[String], val tidSet: Set[Long]) extends Serializable {
  // mixset type, 0 for tidset and 1 for diffset
  var tidType: Int = 0
  var diffSet: Set[Long] = Set()

  def this(item: String, tid: Long) = this(Array(item), Set(tid))

  def +(nextItem: Item): Item = {
    new Item(this.items, this.tidSet ++ nextItem.tidSet)
  }

  def sup(): Int = {
    if (tidType == 1) {
      return diffSet.size
    }

    return tidSet.size
  }

  // choose between tidset and diffset for a better performance
  // just for first level ?
  def optimize(allTrans: Set[Long]) {
    diffSet = allTrans -- tidSet

    if (diffSet.size < tidSet.size)
      tidType = 1
  }

  override def toString(): String = {
    if (tidType == 0)
      "(" + this.items.mkString + ":" + this.tidSet.mkString + ")"
    else
      "(" + this.items.mkString + ":" + this.diffSet.mkString + ")"
  }
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

  // convert the transaction strings to Item
  def toItem(trans: Array[String], frequents: Array[String]): Array[Item] = {
    val TID = trans(0).toLong
    val itemString = trans.drop(1)

    val itemArray = for {item <- itemString
                         if frequents.indexOf(item) > -1} yield (new Item(item, TID))
    return itemArray
  }

  // get frequent items with their mixset
  def mrCountingItems(transactions: RDD[Array[String]], minSupCount: Long): RDD[Item] = {
    val frequents = transactions.flatMap(trans => trans.drop(1).map(item => (item, 1L))).
      reduceByKey(_+_).filter(_._2 >= minSupCount).map(_._1).collect()

    val f1_items = transactions.flatMap(trans => toItem(trans, frequents).map(item => (item.items.mkString, item))).
      reduceByKey(_+_).filter(_._2.sup >= minSupCount).map(_._2).cache

    val allTIDs = transactions.map(trans => trans(0).toLong).collect().toSet
    f1_items.map(_.optimize(allTIDs))

    return f1_items
  }
  //def mrLargeK
  //def mrMiningSubtree
}
