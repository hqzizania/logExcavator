package org.alitouka.spark.dbscan.spatial.box

import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.DbscanSettings
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._
import scala.collection.parallel.ParIterable
import org.alitouka.spark.dbscan.util.debug.Clock
import org.alitouka.spark.dbscan.spatial.DistanceCalculation
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension
import org.alitouka.spark.dbscan.spatial.RegionCalculator
import org.alitouka.spark.dbscan.spatial.TreeItemWithPoints
import org.alitouka.spark.dbscan.spatial.RegionPartitionIndex
import org.alitouka.spark.dbscan.spatial.Region
import org.alitouka.spark.dbscan.spatial.TreeItemWithPoints

/** An indexing data structure which allows fast lookup of point's neighbors
  *
  * Organizes points into a multi-level hierarchy of box-shaped regions. When asked to find point's neighbors,
  * searches for them in the region which contains the specified point and, if necessary,
  * in regions adjacent to it
  *
  * @param partitionBounds A box which embraces all points to be indexed
  * @param dbscanSettings Clustering settings
  * @param partitioningSettings Partitioning settings
  */
private [dbscan] class BoxPartitionIndex (partitionBounds: Region,
  dbscanSettings: DbscanSettings,
  partitioningSettings: PartitioningSettings) extends RegionPartitionIndex(partitionBounds,
      dbscanSettings, partitioningSettings) with DistanceCalculation {

  implicit val distanceMeasure = dbscanSettings.distanceMeasureSuite.distanceMeasure

  private [dbscan] val boxesTree = BoxPartitionIndex.buildTree (region.asInstanceOf[Box], partitioningSettings, dbscanSettings)
  private val largeBox = BoxPartitionIndex.createBoxTwiceLargerThanLeaf(boxesTree)

  /** Finds point's neighbors
    *
    * @param pt A point whose neighbors shuold be found
    * @return A collection of pt's neighbors
    */
  override def findClosePoints (pt: Point): Iterable[Point] = {
    findPotentiallyClosePoints(pt).filter ( p => p.pointId != pt.pointId && calculateDistance (p, pt) <= dbscanSettings.epsilon )
  }


  private [dbscan] def findPotentiallyClosePoints (pt: Point): Iterable[Point] = {
    val box1 = findBoxForPoint(pt, boxesTree)
    var result = ListBuffer[Point] ()

    result ++= box1.points.filter ( p => p.pointId != pt.pointId && Math.abs(p.distanceFromOrigin - pt.distanceFromOrigin) <= dbscanSettings.epsilon )

    if (this.isPointCloseToAnyBound(pt, box1.region, dbscanSettings.epsilon)) {

      box1.adjacentBoxes.foreach {
        box2 => {
          val tempBox = Box (pt, largeBox)

          if (tempBox.isPointWithin(box2.region.asInstanceOf[Box].centerPoint)) {
            result ++= box2.points.filter ( p => Math.abs(p.distanceFromOrigin - pt.distanceFromOrigin) <= dbscanSettings.epsilon )
          }
        }
      }
    }

    result
  }
  
}


private [box] object BoxPartitionIndex extends DistanceCalculation {


  def buildTree (boundingBox: Box,
                 partitioningSettings: PartitioningSettings,
                 dbscanSettings: DbscanSettings): TreeItemWithPoints = {

    val tempTree = RegionCalculator.computeTreeForPartitionIndex(boundingBox,
                 partitioningSettings,
                 dbscanSettings)
   var leafs=List[TreeItemWithPoints](tempTree)
   var level = 0
   while(leafs.flatMap(x => x.children).size>0){
     leafs=leafs.flatMap(x => x.children)
     level+=1
   }
   leafs.foreach {
      leaf => leaf.adjacentBoxes ++= findAdjacentBoxes(leaf, leafs)
    }
   tempTree
  }

  
  def generateEmbracingBox (subitems: Iterable[TreeItemWithPoints], numberOfDimensions: Int): Box = {

    var dimensions: ArrayBuffer[BoundsInOneDimension] = ArrayBuffer[BoundsInOneDimension] ()

    (0 until numberOfDimensions).foreach {
      i => {
        val zeroValue = new BoundsInOneDimension(Double.MaxValue, Double.MinValue, false)
        val x = subitems.map ( _.region.bounds(i) )
        val newDimension = x.fold(zeroValue) {
          (a,b) =>
            new BoundsInOneDimension (
              Math.min (a.lower, b.lower),
              Math.max (a.upper, b.upper),
              a.includeHigherBound || b.includeHigherBound
            )
        }

        dimensions += newDimension
      }
    }

    new Box (dimensions.toArray)
  }


  def generateAndSortBoxes (boundingBox: Box, partitioningSettings: PartitioningSettings, dbscanSettings: DbscanSettings): Array [Box] = {
    RegionCalculator
      .splitRegionIntoEqualRegions(boundingBox, partitioningSettings.numberOfSplitsWithinPartition, dbscanSettings)
      .toArray
      .sortWith(_<_)
    
  }
  


  def findAdjacentBoxes (x: TreeItemWithPoints, boxes: Iterable[TreeItemWithPoints])
    :Iterable[TreeItemWithPoints] = {

    var result: List[TreeItemWithPoints] = Nil

    boxes.filter ( _ != x).foreach {
      y => {

        // The code below relies on the fact that all boxes are of equal size
        // It works a little faster than Box.isAdjacentToBox

        /*var n = 0

        for (i <- 0 until x.region.bounds.size) {
          val cx = x.region.asInstanceOf[Box].centerPoint.coordinates(i)
          val cy = y.region.asInstanceOf[Box].centerPoint.coordinates(i)
          val d = Math.abs (cx - cy)

          if ((d ~~ 0) || (d ~~ x.region.bounds(i).length)) {
            n += 1
          }
        }

        if (n == x.region.bounds.size) {
          result = y :: result
        }*/
        if(x.region.asInstanceOf[Box].isAdjacentToPartition(y.region.asInstanceOf[Box])){
          result = y :: result
        }
      }
    }

    result
  }


  def createBoxTwiceLargerThanLeaf (root: TreeItemWithPoints): Box = {
    val leaf = findFirstLeafBox(root)
    leaf.extendBySizeOfOtherPartition(leaf).asInstanceOf[Box]
  }


  def findFirstLeafBox (root: TreeItemWithPoints): Box = {
    var result = root

    while (!result.children.isEmpty) {
      result = result.children(0)
    }

    result.region.asInstanceOf[Box]
  }
}
