/**
  * Created by zhangh15 on 9/8/16.
  */

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class PEclatDrive extends Serializable {
  var results = ArrayBuffer[Int]()

  def run(transactions: RDD[Array[String]], minSup: Long): Unit = {
    val f1_items = genFreqSingletons(transactions, minSup)
  }

  def show(): Unit = {
    println("frequent items:")
  }

  def genFreqSingletons(transactions: RDD[Array[String]], minSup: Long): Array[String] = {

    println(transactions.flatMap(_.drop(1)).collect.toString)

    val f1_items = transactions.
      flatMap(_.drop(1).map(i => (i, 1))).
      reduceByKey(_+_).
      collect().
      filter(_._2 >= minSup).
      sortBy(_._2)

    f1_items.map(_._1)
  }

}
