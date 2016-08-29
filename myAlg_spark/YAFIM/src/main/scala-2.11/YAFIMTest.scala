import org.apache.spark.{SparkConf, SparkContext}

object YAFIMTest {
  def main(args: Array[String]) {
    val DB = args(0)
    val minSup = args(1).toDouble

    val conf = new SparkConf().setAppName(s"YAFIM Example with $minSup")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(DB).map(_.split(" ")).cache()

    val YAFIMModel = new YAFIMDriver(math.ceil(minSup * transactions.count).toInt)
    YAFIMModel.run(transactions)
    YAFIMModel.show()

    sc.stop()
  }
}
