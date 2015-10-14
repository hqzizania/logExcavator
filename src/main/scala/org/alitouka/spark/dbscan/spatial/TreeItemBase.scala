package org.alitouka.spark.dbscan.spatial

import scala.collection.mutable.ArrayBuffer


private [dbscan] abstract class TreeItemBase [T <: TreeItemBase[_]] (val region: Region) extends Serializable {
  var children: List[T] = Nil

  def flatten [X <: TreeItemBase[_]]: Iterable[X] = {
    this.asInstanceOf[X] :: children.flatMap ( x => x.flatten[X] ).toList
  }

  def flattenBoxes: Iterable[Region] = {
    flatten [TreeItemBase[T]].map { x => x.region }
  }

  def flattenBoxes (predicate: T => Boolean): Iterable [Region] = {
    val result = ArrayBuffer[Region] ()

    flattenBoxes(predicate, result)

    result
  }

  private def flattenBoxes [X <: TreeItemBase[_]] (predicate: X => Boolean, buffer: ArrayBuffer[Region]): Unit = {

    if (!children.isEmpty && children.exists ( x => predicate (x.asInstanceOf[X]))) {
      children.foreach ( x => x.flattenBoxes[X](predicate, buffer))
    }
    else {
      buffer += this.region
    }
  }
}
