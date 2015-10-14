package org.alitouka.spark.dbscan.spatial.sector

import org.alitouka.spark.dbscan.spatial.{BoundsInOneDimension,DistanceCalculation,RegionCalculator,Point,TreeItemWithPoints,RegionPartitionIndex,Region}
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.DbscanSettings
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._

class HyperSectorPartitionIndexer (hyperSector:Region, dbscanSettings: DbscanSettings, 
  partitioningSettings: PartitioningSettings) 
  extends RegionPartitionIndex(hyperSector,dbscanSettings,partitioningSettings)
  with DistanceCalculation {
  
  val numberOfDimensions = region.bounds.length
  
  private [dbscan] val boxesTree: TreeItemWithPoints = {
    
   val tempTree = RegionCalculator.computeTreeForPartitionIndex(region.asInstanceOf[HyperSector],
                 partitioningSettings,
                 dbscanSettings)
   var leafs=List[TreeItemWithPoints](tempTree)
   var level = 0
   while(leafs.flatMap(x => x.children).size>0){
     leafs=leafs.flatMap(x => x.children)
     level+=1
   }
   leafs.foreach {
      leaf => leaf.adjacentBoxes ++= HyperSectorPartitionIndexer.findAdjacentBoxes(leaf, leafs)
    }
    tempTree
    
  }
  
  

  def findClosePoints(pt: Point): Iterable[Point] = {
    
    // to improve performance by reducing the amount of memory used
    // we may push up the filter about the exact distance measure
    // in the two filter above
    
    val hs = findBoxForPoint(pt, boxesTree)
    
    //Console.out.println("Here I have "+hs.points.size+" adjacent points...")
    
    val adjacentPoints = ListBuffer[Point]()
    
    //hs.points.foreach { x => Console.out.println("Relative distance from the origin is "+Math.abs(x.distanceFromOrigin - pt.distanceFromOrigin)) }
    
    adjacentPoints ++= hs.points.filter { x => x.pointId!=pt.pointId && Math.abs(x.distanceFromOrigin - pt.distanceFromOrigin) <~ dbscanSettings.epsilon }
    
    //Console.out.print("Here I have "+adjacentPoints.size+" adjacent points...")
    
    if(isPointCloseToAnyBound(pt, hs.region, dbscanSettings.epsilon)){
      
      for(r <- hs.adjacentBoxes){
        adjacentPoints++=r.points.filter { x => Math.abs(x.distanceFromOrigin - pt.distanceFromOrigin) <~ dbscanSettings.epsilon } 
      }
      
      
    }
    implicit val distanceMeasure = dbscanSettings.distanceMeasureSuite.distanceMeasure
    adjacentPoints.filter { x => calculateDistance(pt, x) <= dbscanSettings.epsilon }
  }

  
}

private [sector] object HyperSectorPartitionIndexer{
  
  def findAdjacentBoxes(leaf:TreeItemWithPoints, leafs:List[TreeItemWithPoints]) : Iterable[TreeItemWithPoints] = {

    var res : List[TreeItemWithPoints] = Nil 
    leafs.withFilter { _ != leaf }.foreach{ l =>
      if(leaf.region.isAdjacentToPartition(l.region)){
        res=l::res
      }
    }
    res
  }

  def generateEmbracingBox(leafsSubset: Iterable[TreeItemWithPoints], numberOfDimensions: Int) : HyperSector = {
    var dimensions: ArrayBuffer[BoundsInOneDimension] = ArrayBuffer[BoundsInOneDimension] ()

    (0 until numberOfDimensions).foreach {
      i => {
        val zeroValue = new BoundsInOneDimension(Double.MaxValue, Double.MinValue, false)
        val x = leafsSubset.map ( _.region.bounds(i) )
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

    new HyperSector (dimensions.toArray)
  }
  
}