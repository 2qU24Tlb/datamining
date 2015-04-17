package myAlg.spark.svt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SVT {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Scale Vertical Mining")
    val sc = new SparkContext(sparkConf )

    run()

    sc.stop()
  }

  def run() {
    println("Hello, world!")
  }
}
