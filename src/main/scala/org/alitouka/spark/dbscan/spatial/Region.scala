package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.RegionId
import org.alitouka.spark.dbscan.DbscanSettings

abstract class Region (val bounds: Array[BoundsInOneDimension], 
    val regionId: RegionId = 0, val partitionId: Int = -1, var adjacentPartitions: List[Region] = Nil) 
    extends Serializable with Ordered[Region]{
  
  def this (b: List[BoundsInOneDimension], boxId: Int) = this (b.toArray, boxId)

  def this (b: Region) = this (b.bounds, b.regionId, b.partitionId, b.adjacentPartitions)

  def this (b: BoundsInOneDimension*) = this (b.toArray)
  
  
  def splitAlongLongestDimension(numberOfSplits: Int,
      idGenerator: RegionIdGenerator = new RegionIdGenerator(this.regionId)): Iterable[Region]
  
  
  def isPointWithin (pt: Point) : Boolean
  
  
  def isBigEnough (settings: DbscanSettings): Boolean
  
  //def extendBySizeOfOtherPartition (b: Region): Region
  
  def withId (newId: RegionId) : Region 

  def withPartitionId (newPartitionId: Int) : Region 
  
  def addAdjacentPartition (b: Region)
  
  def isAdjacentToPartition (that: Region): Boolean
  
  def isPointCloseToAnyBounds(point:Point, threshold:Double) : Boolean

  def calculateBoxSize: Double
  
}