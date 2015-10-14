package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.util.debug.Clock

abstract class RegionPartitionIndex (val region:Region, val dbscanSettings: DbscanSettings,
  val partitioningSettings: PartitioningSettings) extends Serializable {
  
  private [dbscan] val boxesTree:TreeItemWithPoints
  
  def populate [T <: Iterable[Point]] (points: T): Unit = {
    populate (points.iterator)
  }

  def populate (points: Array[Point]): Unit = {
    populate (points.iterator)
  }
  
  def populate (points: Iterator[Point]): Unit = {
    val clock = new Clock ()

    points.foreach {
      pt => {
        findBoxAndAddPoint(pt, boxesTree)
      }
    }

    clock.logTimeSinceStart("Population of partition index")
  }
  
  def findClosePoints (pt: Point): Iterable[Point]
  
  private [dbscan] def findBoxForPoint (pt: Point, root: TreeItemWithPoints): TreeItemWithPoints = {
    if (root.children.isEmpty) {
      root
    }
    else {
     val child = root.children.find ( x => x.region.isPointWithin(pt) )

      child match {
        case b: Some[TreeItemWithPoints] => findBoxForPoint (pt, b.get)
        case _ => {
          throw new Exception (s"Box for point $pt was not found")
        }
      }
    }
  }
  
  private def findBoxAndAddPoint (pt: Point, root: TreeItemWithPoints): Unit = {
    val b = findBoxForPoint(pt, root)
    b.points += pt
  }
  
}