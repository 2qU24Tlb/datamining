class Itemset(val items: Array[Int], val support: Int) extends Serializable with Ordered[Itemset]{
  val itemset: Array[Int] = items
  var sup: Int = support

  def this(newItem: Int) = this(Array(newItem), 1)
  def this(newItem: Int, newSupport: Int) = this(Array(newItem), newSupport)
  def this(newItems: Array[Int]) = this(newItems, 1)

  def prefix() = items.take(this.items.length - 1)
  def last() = items.last

  def +(another: Itemset): Itemset = {
    new Itemset(this.items, this.support + another.support)
  }

  override def compare(that: Itemset): Int = {
    for (i <- items.indices) {
      if (this.items(i) > that.items(i))
        return 1
      else if (this.items(i) < that.items(i))
        return -1
    }
    0
  }

  override def toString(): String = {
    "items: <" + items.mkString(" ") + ">, " + support.toString
  }

}
