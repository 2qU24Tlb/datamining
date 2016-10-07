import org.apache.spark.{SparkConf, SparkContext}

object PEclatLaunch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"PEclat Example with minSup=$args(1)")
    val sc = new SparkContext(conf)

    val transactions = sc.textFile(args(0)).map(_.trim.split("\\s+")).cache
    val minSup = Math.ceil(args(1).toDouble * transactions.count)

    val PEclatModel = new PEclatDrive()
    PEclatModel.run(transactions, minSup)
    PEclatModel.show()

    sc.stop()
  }

}
