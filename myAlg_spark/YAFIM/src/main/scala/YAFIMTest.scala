package unioah.spark.fpm

import org.apache.spark.{SparkConf, SparkContext}

object YAFIMtest {
  def main(args: Array[String]) {
    val minSup = args(0).toDouble
    val NumPartitions = -1
    val DB = "file:/tmp/retail.txt"

    val conf = new SparkConf().setAppName(s"FPGrowthExample with $minSup")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(DB).map(_.split(" ")).cache()

    sc.stop()
  }
}
