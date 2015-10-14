package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.util.collection.SynchronizedArrayBuffer

private [dbscan] class TreeItemWithPoints(b: Region,
  val points: SynchronizedArrayBuffer[Point] = new SynchronizedArrayBuffer[Point] (),
  val adjacentBoxes: SynchronizedArrayBuffer[TreeItemWithPoints] = new SynchronizedArrayBuffer[TreeItemWithPoints] ())
  extends TreeItemBase [TreeItemWithPoints] (b) {
}
