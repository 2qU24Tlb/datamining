import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

class PEclatDrive () extends Serializable {
  var results = ArrayBuffer[Int]


  def run(Transactions: RDD[Array[String]], minSup: Long) {

  }


  def genFreqSingletons (transactions: RDD[Array[String]]): Array[] = {
  }

}
