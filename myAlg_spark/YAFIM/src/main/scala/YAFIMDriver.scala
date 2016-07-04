package unioah.spark.fpm

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
//import org.apache.jorphan.collections
import unioah.spark.fpm.Itemset

class YAFIM(val minSup: Int) extends Serializable {

  def run[Item: ClassTag] (data: RDD[Array[Item]]) {
    f1_items = genFreqSingletons(data, minSup)
  }

  def show() {
  }

  // Phase I
  def genFreqSingletons[data: ClassTag](transactions: RDD[Array[data]], minSup: Int): Array[Itemset] = {
    val f1_items = transactions.flatMap{x -> x}
      .map(i => (i, 1L))
      .reduceByKey(_ + _)
      .filter(_._2 >= minSup)
      .sortBy(_._2)
      .map(new Itemset(_._1))

    return f1_items
  }

  // Phase II
  def genFreItemsets[data: ClassTag](transactions: RDD[Array[data]], minSup: Int, freqItem: Array[data]) {
  }
}
