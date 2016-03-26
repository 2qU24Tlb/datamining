import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.RangePartitioner


// start of data structure
class VertItem(item: Array[Long], TIDs: Array[Long]) extends Serializable {
  val _item = item.sorted

  val _TIDs = TIDs.sorted
  val prefix = (_item.take(_item.length - 1).mkString(","))
  var support = TIDs.length

  def +(another: VertItem) = {
    new VertItem(this._item, (this._TIDs ++ another._TIDs).distinct.sorted)
  }
  def intersect(another: VertItem) = {
    new VertItem((this._item ++ another._item).distinct.sorted,
      (this._TIDs.intersect(another._TIDs)).sorted)
  }
  // if superset is not using diff form
  def diff(another: VertItem): VertItem = {
    val newItem = new VertItem((this._item ++ another._item).distinct.sorted,
      (this._TIDs.diff(another._TIDs)).sorted)
    newItem.support = this.support - newItem.support
    newItem
  }
  // if superset is already using diff form
  def diff2(another: VertItem): VertItem = {
    val newItem = new VertItem((this._item ++ another._item).distinct.sorted,
      (another._TIDs.diff(this._TIDs)).sorted)
    newItem.support = this.support - newItem.support
    newItem
  }
  override def toString(): String = {
    "(" + _item.mkString(",") + ")" + ":" + _TIDs.mkString(",")
  }
}

object Utils {
  var debug = false
  var result = ArrayBuffer[String] ()

  // check partition status
  def checkParttion[T] (part: RDD[T]) {
    part.mapPartitionsWithIndex((idx, itr) => itr.map(s => (idx, s))).collect.foreach(println)
  }

  def addResult(item: String) {
    if (debug)
      result.append(item)
  }

  def showResult() {
    if (debug)
      for (i <- result)
        println(i)
    else
      println("done!")
  }

  //function: write results to log file
}

// start of driver section
class SVTDriver(transactions: RDD[Array[Long]], minSup: Double) extends Serializable {
  val _dbLength: Long = transactions.count
  val _rminSup: Long = (minSup * _dbLength).ceil.toLong
  var partitionSize: Int = 0

  def run() {
    println("Number of Transactions: " + _dbLength.toString)

    // step 1
    val _freq1Items = genFreqSingletons(transactions, _rminSup).cache

    // step 2
    val _freq2itemsets = genKItemsets(_freq1Items, _rminSup).cache
    val _freq3itemsets = genKItemsets(_freq2itemsets, _rminSup).cache
    val _freq4itemsets = genKItemsets(_freq3itemsets, _rminSup).cache
    val _freq5itemsets = genKItemsets(_freq4itemsets, _rminSup).cache
    val _EClass = rePartition(_freq5itemsets)

    // step 3
    val _freqSets = localVM(_EClass, _rminSup)

    Utils.showResult
  }

  //generate local frequent items in each partition
  def genFreqSingletons(DB: RDD[Array[Long]], rminSup: Long): RDD[VertItem] = {
    val _localItems = DB.mapPartitions(genItems)
    val _globalItems = _localItems
      .map(x => (x._1, x._2.length))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .map(_._1)
      .collect

    _globalItems.foreach(println)

    val localResult =  _localItems.filter(x => _globalItems.contains(x._1))
      .map(x => new VertItem(Array(x._1), x._2))
    localResult
  }

  // generate first level of equivalent class from singletons
  def genKItemsets(singletons: RDD[VertItem], rminSup: Long): RDD[VertItem] = {
    val _localEclass  = singletons.mapPartitions(genEclass)
    val _globalItems = _localEclass
      .map(x => (x._item.mkString(","), x.support))
      .reduceByKey(_ + _)
      .filter(_._2 >= rminSup)
      .map(_._1)
      .collect

    partitionSize = _globalItems.length
    _globalItems.foreach(println)

    val localFrequent =  _localEclass.filter(
      x => _globalItems.contains(x._item.mkString(",")))
    localFrequent
  }

  def rePartition(kItemsets: RDD[VertItem]): RDD[VertItem]={
    // [BugFix] we need to calculate the size of freqEclass that fits the memory
    val reMap = kItemsets.keyBy(x => x.prefix)
    val localResult = reMap.partitionBy(
      new RangePartitioner[String, VertItem](kItemsets.count.toInt, reMap))
      .map(_._2)
      .mapPartitions(combineSame)
      .map(_._2)

    localResult
  }

  // combine same items in a same partition
  def combineSame(iter: Iterator[VertItem]): Iterator[(String, VertItem)] = {
    val _EclassList = HashMap.empty[String, VertItem]

    while (iter.hasNext) {
      var cur = iter.next
      var key = cur._item.mkString(",")

      if (_EclassList.contains(key)) {
        _EclassList += (key -> (_EclassList(key) + cur))
      } else {
        _EclassList += (key -> cur)
      }
    }
    _EclassList.toList.iterator
  }

  // generate inheritors from equivalent class
  def localVM(Eclass: RDD[VertItem], rminSup: Long): RDD[VertItem] = {
    val _localEclass  = Eclass.mapPartitions(genSets)
    _localEclass.map(x => x._item.mkString(",")+":"+x.support.toString).collect.foreach(println)

    _localEclass
  }

  // transform horizontal database to vertical form
  def genItems(iter: Iterator[Array[Long]]) : Iterator[(Long, Array[Long])] = {
    val _item2TID = HashMap.empty[Long, ArrayBuffer[Long]]

    var cur = Array[Long]()
    while (iter.hasNext) {
      cur = iter.next
      for (i <- cur.tail) {
        if (_item2TID.contains(i))
          _item2TID(i).append(cur.head)
        else
          _item2TID  += (i -> ArrayBuffer(cur.head))
      }
    }
    val _result = _item2TID.toList
      .sortBy(x => x._1)
      .map(x => (x._1, x._2.toArray))

    _result.iterator
  }

  def genEclass(iter: Iterator[VertItem]): Iterator[VertItem] = {
    val vList = iter.toList
    var i, j = 0

    var result = for (i <- 0 to vList.length - 1;
      j <- i + 1 to vList.length - 1;
      if (vList(i).prefix == vList(j).prefix)) yield {
      vList(i).intersect(vList(j))
    }
    result = result.filter(x => x.support != 0)
    result.iterator
  }

  def genSets(iter: Iterator[VertItem]): Iterator[VertItem] = {
    var superSet = iter.toList
    var i, j = 0

    var subSet = for (i <- 0 to superSet.length - 1;
      j <- i + 1 to superSet.length - 1;
      if (superSet(i).prefix == superSet(j).prefix)) yield {
      superSet(i).intersect(superSet(j))
    }
    subSet = subSet.filter(x => x.support >= _rminSup)

    // 0 -- will use eclat
    // 1 -- will use declat, superset is using intersection
    // 2 -- will use declat, superset is using differences
    var declat = 0

    while(subSet.length > 1) {
      if (declat == 0) {
        // calculate the density of database
        if (subSet.length * 2 >= superSet.length) {
          declat = 1
        }
      }
      superSet = subSet.toList

      subSet = for (i <- 0 to superSet.length - 1;
        j <- i + 1 to superSet.length - 1;
        if (superSet(i).prefix == superSet(j).prefix)) yield {
        if (declat == 0) {
          superSet(i).intersect(superSet(j))
        } else if (declat == 1){
          superSet(i).diff(superSet(j))
        } else {
          superSet(i).diff2(superSet(j))
        }
      }

      subSet = subSet.filter(x => x.support >= _rminSup)

      if (declat == 1) {
        declat = 2
      }
    }

    subSet.iterator
  }
}

// start of test
object MyTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Scale Vertical Mining example")
    val sc = new SparkContext(conf)

    val DB = sc.textFile(args(0)).map(_.split("\\s+").map(_.toLong)).cache
    val minSup = args(1).toDouble

    val model = new SVTDriver(DB, minSup)
    Utils.debug = false
    model.run()

    sc.stop
  }
}
