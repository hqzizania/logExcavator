package org.alitouka.spark.dbscan.spatial.box

import org.alitouka.spark.dbscan.{DbscanSettings, RegionId}
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._
import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension
import org.alitouka.spark.dbscan.spatial.Region
import org.alitouka.spark.dbscan.spatial.RegionIdGenerator
import org.alitouka.spark.dbscan.spatial.Point

/** Represents a box-shaped region in multi-dimensional space
  *
  * @param bounds Bounds of this box in each dimension
  * @param boxId A unique identifier of this box
  * @param partitionId Identifier of a data set partition which corresponds to this box
  */
private [dbscan] class Box (bounds: Array[BoundsInOneDimension], boxId: RegionId = 0, partitionId: Int = -1, adjacentBoxes: List[Box] = Nil) 
  extends Region (bounds, boxId, partitionId, adjacentBoxes) {

  val centerPoint = calculateCenter (bounds)

  def this (b: List[BoundsInOneDimension], boxId: Int) = this (b.toArray, boxId)

  def this (b: Box) = this (b.bounds, b.regionId, b.partitionId, b.adjacentPartitions.asInstanceOf[List[Box]])

  def this (b: BoundsInOneDimension*) = this (b.toArray)

  def splitAlongLongestDimension (numberOfSplits: Int,
      idGenerator: RegionIdGenerator = new RegionIdGenerator(this.boxId)): Iterable[Box] = {

    val (longestDimension, idx) = findLongestDimensionAndItsIndex()

    val beforeLongest = if (idx > 0) bounds.take (idx) else Array[BoundsInOneDimension] ()
    val afterLongest = if (idx < bounds.size-1) bounds.drop(idx+1) else Array[BoundsInOneDimension] ()
    val splits = longestDimension.split(numberOfSplits)

    splits.map {
      s => {
        val newBounds = (beforeLongest :+ s) ++: afterLongest
        new Box (newBounds, idGenerator.getNextId())
      }
    }
  }

  def isPointWithin (pt: Point) = {

    assert (bounds.length == pt.coordinates.length)

    bounds.zip (pt.coordinates).forall( x => x._1.isNumberWithin(x._2) )
  }

  def isBigEnough (settings: DbscanSettings): Boolean = {

    bounds.forall( _.length >= 2*settings.epsilon/100 )
  }

  def extendBySizeOfOtherPartition (b: Region): Region = {

    assert (this.bounds.size == b.bounds.size)

    val newBounds = this.bounds.zip (b.bounds).map ( x => x._1.extend(x._2) )
    new Box (newBounds)
  }

  def withId (newId: RegionId): Region = {
    new Box (this.bounds, newId, this.partitionId, this.adjacentBoxes)
  }

  def withPartitionId (newPartitionId: Int): Region = {
    new Box (this.bounds, this.boxId, newPartitionId, this.adjacentBoxes)
  }

  override def toString (): String = {
    "Box " + bounds.mkString(", ") + "; id = " + boxId + "; partition = " + partitionId
  }

  private [dbscan] def findLongestDimensionAndItsIndex ()
    :(BoundsInOneDimension, Int) = {

    var idx: Int = 0
    var foundBound: BoundsInOneDimension = null
    var maxLen: Double = Double.MinValue

    for (i <- 0 until bounds.size) {
      val b = bounds(i)

      val len = b.length

      if (len > maxLen) {
        foundBound = b
        idx = i
        maxLen = len
      }
    }

    (foundBound, idx)
  }

  private [dbscan] def calculateCenter (b: Array[BoundsInOneDimension]): Point = {
    val centerCoordinates = b.map ( x => x.lower + (x.upper - x.lower) / 2 )
    new Point (centerCoordinates)
  }

  def addAdjacentPartition (b: Region) = {
    adjacentPartitions = b :: adjacentPartitions
  }

  override def compare(that: Region): Int = {
    assert (this.bounds.size == that.bounds.size)
    
    centerPoint.compareTo(that.asInstanceOf[Box].centerPoint)
  }

  def isAdjacentToPartition (that: Region): Boolean = {

    assert (this.bounds.size == that.bounds.size)

    val (adjacentBounds, notAdjacentBounds) = this.bounds.zip(that.bounds).partition {
      x => {
        x._1.lower ~~ x._2.lower ||
        x._1.lower ~~ x._2.upper ||
        x._1.upper ~~ x._2.upper ||
        x._1.upper ~~ x._2.lower
      }
    }

    adjacentBounds.size >= 1 && notAdjacentBounds.forall {
      x => {
        (x._1.lower >~ x._2.lower && x._1.upper <~ x._2.upper) || (x._2.lower >~ x._1.lower && x._2.upper <~ x._1.upper)
      }
    }
  }

  def isPointCloseToAnyBounds(point: Point, threshold: Double): Boolean = {
      (0 until point.coordinates.size).exists( i => isPointCloseToBound (point, this.bounds(i), i, threshold))

  }
  
  protected def isPointCloseToBound (pt: Point, bound: BoundsInOneDimension, dimension: Int, threshold: Double)
    :Boolean = {
    val x = pt.coordinates(dimension)
    Math.abs(x - bound.lower) <= threshold || Math.abs(x - bound.upper) <= threshold
  }

  def calculateBoxSize: Double = {
    val size = bounds.map(x => (x.upper - x.lower)).reduce(_+_)
    size
  }
  
}

object Box {
  def apply (centerPoint: Point, size: Box): Box = {

    val newBounds = centerPoint.coordinates.map ( c => new BoundsInOneDimension(c, c, true) ).toArray
    new Box (newBounds).extendBySizeOfOtherPartition(size).asInstanceOf[Box]
  }
}