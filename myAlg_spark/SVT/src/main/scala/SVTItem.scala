package unioah.spark.fpm

class SVTItem(val items: Array[String], val TIDs: Set[Long], val sup: Long) extends Serializable {
  val prefix = items.take(items.size - 1).mkString
  var TIDtype: Int = 1 // 1 for tidSet, 2 for diffSet

  def this(item: String, TID: Long) = this(Array(item), Set(TID), 1l)

  def +(another: SVTItem): SVTItem = {
    new SVTItem(this.items, this.TIDs ++ another.TIDs, this.sup + another.sup)
  }

  def &(another: SVTItem): SVTItem = {
    val newTIDs = this.TIDs & another.TIDs
    val newItems = (this.items.toSet ++ another.items.toSet).toArray.sorted
    val newSup = newTIDs.size.toLong

    return new SVTItem(newItems, newTIDs, newSup)
  }

  override def toString(): String = {
    //"(" + this.items.mkString(".") + ":" + this.TIDs.toList.sorted.mkString("_") + ")"
    "(" + this.items.mkString(".") + ":" + this.TIDs.size.toString() + ")"
  }
}
