package org.alitouka.spark.dbscan.spatial

import scala.collection.mutable.ArrayBuffer

private [dbscan] class TreeItemWithNumberOfPoints(b: Region) extends TreeItemBase [TreeItemWithNumberOfPoints] (b) {

  var numberOfPoints: Long = 0

  override def clone (): TreeItemWithNumberOfPoints = {

    val result = new TreeItemWithNumberOfPoints (this.region)
    result.children = this.children.map { x => x.clone () }.toList

    result
  }

}
