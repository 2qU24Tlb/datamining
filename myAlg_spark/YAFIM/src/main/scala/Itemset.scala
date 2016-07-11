package unioah.spark.fpm

class Itemset(items: Array[Int], support: Int) extends Serializable {
  val itemset: Array[Int] = items
  var sup: Int = support

  def this(newItem: Int) = this(Array(newItem), 1)
  def this(newItem: Int, newSupport: Int) = this(Array(newItem), newSupport)
  def this(newItems: Array[Int]) = this(newItems, 1)

  def prefix() = itemset.take(this.itemset.length - 1)
  def last() = itemset.last

  override def toString(): String = {
    "items: " + itemset.mkString + ": " + sup.toString
  }
}
