import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Peclat {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Peclat")
    val sc = new SparkContext(conf)

    val data = sc.textFile("file:/tmp/sampledb", 2)
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val minSup = 0.5 // user defined min support
    val minSupCount = math.ceil(transactions.count * minSup).toLong
  }

  def toItem(trans: Array[String]): Array[Item] = {
    val TID = trans(0).toLong
    val items = trans.drop(1)

    val itemArray = for (item <- items) yield (new Item(TID, item))
    itemArray
  }

  def mrCountingItems(data: RDD[Array[String]], minSup: Long): RDD[Array[Item]] = {
    tid = data[0]
    data.flatMap(x => x.drop(1).map(y => new Item (y, x.head)))

    val f1 = data.flatMap(_.map(item => (item, 1L))).
      reduceByKey(_+_).
      filter(_._2 >= minSup).
      map(_._1)

    f1.collect()
  }
  //def mrLargeK
  //def mrMiningSubtree
}

class Item(val TIDs: Array[Long], val itemsets: Array[String]) extends Serializable{

  def this(TID: Long, itemset: String) = this(Array(TID), Array(itemset))

  override def toString(): String =
    "(" + this.TIDs.mkString + ":" + this.itemsets.mkString + ")"
}
