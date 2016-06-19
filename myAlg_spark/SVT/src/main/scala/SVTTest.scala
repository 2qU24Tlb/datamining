import org.apache.spark.{SparkConf, SparkContext}
import unioah.spark.fpm.SVT

object SVTTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SVT")
    val sc = new SparkContext(conf)

    // args(0) for transactions, args(1) for minSup
    val data = sc.textFile("file:/tmp/T10I4D100K_lined.dat")
    val transactions = data.map(s => s.trim.split("\\s+")).cache
    val minSup = args(0).toDouble // user defined min support

    println("number of transactions: " + transactions.count().toString() + "\n")

    val model = new SVT(minSup)
      .run(transactions)

    //val fk_items = genKItemsets(f1_items, kCount, minSupCount)
    //Utils.showResult("Equivalent Class: ", fk_items)

    //val fre_items = paraMining(fk_items, minSupCount)
    //Utils.showResult("Number of frequent itemsets: ", fre_items)

    sc.stop
  }
}
