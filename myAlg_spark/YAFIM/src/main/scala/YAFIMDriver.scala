package unioah.spark.fpm

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.jorphan.collections

class YAFIM(val minSupport: Int) extends Serializable {

  def run[Item: ClassTag] (data: RDD[Array[Item]]) {
  }

  // Phase I
  def genFreqSingletons[data: ClassTag](transactions: RDD[Array[data]], minSup: Int): Array[data] = {
    val f1_items = transactions.flatMap{_.drop(1)}
      .map(i => (i, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSupCount)
      .collect
      .sortBy(_._2)

    return f1_items
  }

  // Phase II
  def genFreItemsets[data: ClassTag](transactions: RDD[Array[data]], minSup: Int, freqItem: Array[data]) {

  }
}
