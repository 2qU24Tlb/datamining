import unioah.spark.fpm.YAFIM
import org.apache.spark.{SparkConf, SparkContext}

object YAFIMtest {
  def main(args: Array[String]) {
    val minSup = args(0).toDouble
    val NumPartitions = -1
    val DB = "file:/tmp/retail.txt"

    val conf = new SparkConf().setAppName(s"YAFIM Example with $minSup")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(DB).map(_.split(" ")).cache()

    val YAFIMModel = new YAFIM(math.ceil(minSup * transactions.count).toInt)
    YAFIMModel.run(transactions)
    YAFIMModel.show()

    sc.stop()
  }
}
