import unioah.spark.fpm.YAFIMDriver
import org.apache.spark.{SparkConf, SparkContext}

object YAFIMTest {
  def main(args: Array[String]) {
    val minSup = args(0).toDouble
    val NumPartitions = -1
    val DB = "file:/tmp/retail.txt"

    val conf = new SparkConf().setAppName(s"YAFIM Example with $minSup")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(DB).map(_.split(" ")).cache()
    println("number of transaction is: " + transactions.count.toString)

    val YAFIM = new YAFIMDriver(math.ceil(minSup * transactions.count).toInt)
    YAFIM.run(transactions)
    YAFIM.show()

    sc.stop()
  }

}
