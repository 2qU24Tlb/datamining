package unioah.spark.fpm

class Itemset(items: Array[Int], support: Int) extends serializable {
  var itemset: Array[Int] = items
  var sup: Int = support

  def this(newItem: Int) = this(Array(newItem), 1)
  def this(newItems: Array[Int]) = this(newItems, 1)

  def show(): {
    println("items:" + itemset.mkString + ":" + sup)
  }
}
