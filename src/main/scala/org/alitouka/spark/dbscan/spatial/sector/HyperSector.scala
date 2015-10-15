package org.alitouka.spark.dbscan.spatial.sector

import org.alitouka.spark.dbscan.spatial.Region
import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.RegionId
import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.util.math.HyperSphericalCoordinates
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._
import org.alitouka.spark.dbscan.spatial.RegionIdGenerator

class HyperSector(bounds:Array[BoundsInOneDimension], id:RegionId = 0, partitionId:Int = -1, 
    adjacent:List[Region] = Nil) extends Region(bounds, id, partitionId,adjacent) {
  
  def this (b: List[BoundsInOneDimension], boxId: Int) = this (b.toArray, boxId)
  
  
  /**
   * This method works only if the distance measure used 
   * is [[org.alitouka.spark.dbscan.util.math.CosineDistance]].
   * 
   * Indeed, it suppose the epsilon is set in <b>radians</b>.
   *
   */
  override def isBigEnough(settings: DbscanSettings): Boolean = {
    bounds.forall( _.length >= 2*settings.epsilon/10 )
  }

  override def withId(newId: RegionId): Region = {
     new HyperSector(this.bounds, newId, this.partitionId, this.adjacentPartitions)
  }

  override def withPartitionId(newPartitionId: Int): Region = {
     new HyperSector(this.bounds, this.regionId, newPartitionId, this.adjacentPartitions)
  }
  
  override def isPointWithin(pt:Point) : Boolean = {
    val spherCoords=HyperSphericalCoordinates.fromCartesian(pt.coordinates.toArray[Double])
    assert(bounds.length == spherCoords.Phis.length)
    
    bounds.zip (spherCoords.Phis).forall( x => x._1.isNumberWithin(x._2) )
  }
  
  
  /**
   * This method works only if the distance measure used 
   * is [[org.alitouka.spark.dbscan.util.math.CosineDistance]].
   * 
   * Indeed, it suppose the epsilon is set in <b>radians</b>.
   *
   */
  override def isPointCloseToAnyBounds(point: Point, threshold: Double): Boolean = {
    //val radiantThreshold = HyperSector.convertCosineValueToRadiant(threshold)
    val spherCoords=HyperSphericalCoordinates.fromCartesian(point.coordinates.toArray[Double])
    bounds.zip(spherCoords.Phis).exists( t  => Math.abs(t._1.upper-t._2) <= threshold || Math.abs(t._1.lower-t._2) <= threshold  )
  }

  
  
  override def addAdjacentPartition (b: Region) = {
    adjacentPartitions = b :: adjacentPartitions
  }

  override def compare(that: Region): Int = {
    assert ( that.isInstanceOf[HyperSector])
    val other = that.asInstanceOf[HyperSector]
    assert(this.bounds.length == other.bounds.length)
    var i:Int=0
    while ( i<this.bounds.length &&  this.bounds(i).lower ~~ other.bounds(i).lower  ){
      i+=1
    }
    if(this.bounds(i).lower - other.bounds(i).lower>0){
      return 1
    }else if(this.bounds(i).lower - other.bounds(i).lower<0){
      return -1
    }
    return 0
  }

  def splitAlongLongestDimension(numberOfSplits: Int, idGenerator: RegionIdGenerator): Iterable[Region] = {
     var idx: Int = 0
     var longestDimension: BoundsInOneDimension = null
     var maxLen: Double = Double.MinValue
  
     for (i <- 0 until bounds.size) {
       val b = bounds(i)
  
       val len = b.length
  
       if (len > maxLen) {
         longestDimension = b
         idx = i
         maxLen = len
       }
     }
     val beforeLongest = if (idx > 0) bounds.take (idx) else Array[BoundsInOneDimension] ()
     val afterLongest = if (idx < bounds.size-1) bounds.drop(idx+1) else Array[BoundsInOneDimension] ()
     val splits = longestDimension.split(numberOfSplits)
  
     splits.map {
        s => {
          val newBounds = (beforeLongest :+ s) ++: afterLongest
          new HyperSector (newBounds, idGenerator.getNextId())
       }
     }
   }

   def isAdjacentToPartition(that: Region): Boolean = {
     this.bounds.slice(0, this.bounds.size-1)
      .zip(that.bounds.slice(0, that.bounds.size-1)).forall(x => {
        x._1.lower <~ x._2.upper && x._1.upper >~ x._2.lower
        
      }) && ( 
          this.bounds(this.bounds.size-1).lower <~ that.bounds(that.bounds.size-1).upper 
          && this.bounds(this.bounds.size-1).upper >~ that.bounds(that.bounds.size-1).lower
          
          || this.bounds(this.bounds.size-1).lower~~0 && that.bounds(that.bounds.size-1).upper~~(2.0*Math.PI)
          || this.bounds(this.bounds.size-1).upper~~(2.0*Math.PI) && that.bounds(that.bounds.size-1).lower~~0
      )
      
    
   }

  def calculateBoxSize: Double = {
    val size = bounds.map(x => (x.upper - x.lower)).reduce(_+_)
    size
  }
  
  
}

/*
private [sector] object HyperSector{
  
  def convertCosineValueToRadiant(cosineValue:Double) : Double = {
    assert(cosineValue <= 1 && cosineValue >= -1)
    Math.acos(1-cosineValue)
  }
  
}*/