package org.alitouka.spark.dbscan.spatial.sector

import org.alitouka.spark.dbscan.{RawDataSet,RegionId,DbscanSettings}
import org.alitouka.spark.dbscan.spatial.{RegionCalculator,Point,BoundsInOneDimension,Region}
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.util.math.HyperSphericalCoordinates
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.alitouka.spark.dbscan.spatial.rdd.GenericPartitioner

class HyperSectorCalculator (data:RawDataSet) extends RegionCalculator(data) {
  val numberOfDimensions: Int = data.first().coordinates.length

  override def calculateBoundingBox: Region = {
    val minDefaultPoint = new HyperSphericalCoordinates (1.0,Array.fill (numberOfDimensions-1)(Math.PI*2.0))
    val maxDefaultPoint = new HyperSphericalCoordinates (1.0, Array.fill (numberOfDimensions-1)(0.0))
    
    //val maxs = new HyperSphericalCoordinates (1.0,Array.fill (numberOfDimensions-1)(Math.PI))
    //val mins = new HyperSphericalCoordinates (1.0, Array.fill (numberOfDimensions-1)(0.0))
    
    
    val sphericalCoordsRDD = data.map(p => HyperSphericalCoordinates.fromCartesian(p.coordinates.toArray))
    
    val mins = sphericalCoordsRDD.fold(minDefaultPoint)( (min, p) => new HyperSphericalCoordinates(1.0,  min.Phis.zip(p.Phis).map(
      (x) => Math.min(x._1,x._2)    
    ).toArray ) )
    
    val maxs = sphericalCoordsRDD.fold(maxDefaultPoint)( (max, p) => new HyperSphericalCoordinates(1.0,  max.Phis.zip(p.Phis).map(
      (x) => Math.max(x._1,x._2)    
    ).toArray ) )
    mins.Phis(numberOfDimensions-2) = 0
    maxs.Phis(numberOfDimensions-2) = Math.PI*2.0
    
    new HyperSector(mins.Phis.zip(maxs.Phis).map((b) => new BoundsInOneDimension(b._1,b._2,true)).toArray)
    
  }

  override def generateDensityBasedPartitions(partitioningSettings: PartitioningSettings, dbscanSettings: DbscanSettings): (Iterable[Region], Region) = {
    val root = calculateBoundingBox.asInstanceOf[HyperSector]
    val sampleData: Array[Point] = data.takeSample(true, 100, new java.text.SimpleDateFormat("ss").format(new java.util.Date()).toLong)
    val tree = RegionCalculator.generateTreeOfRegions(root, partitioningSettings, dbscanSettings, sampleData)
    val broadcastTree = data.sparkContext.broadcast(tree)
    
    val partialCounts: RDD[(RegionId, Long)] = data.mapPartitions {
      it => {
        val bt = broadcastTree.value.clone ()
        RegionCalculator.countPointsInOneRegion(bt, it)
      }
    }

    val totalCounts = partialCounts.foldByKey(0)(_+_).collectAsMap()
    val sectorsWithEnoughPoints = tree.flattenBoxes {
      x => totalCounts (x.region.regionId) >= partitioningSettings.numberOfPointsInBox
    }

    RegionCalculator.assignAdjacentRegions (sectorsWithEnoughPoints)

    (GenericPartitioner.assignPartitionIdsToBoxes(sectorsWithEnoughPoints), root)
    
    
  }
  
  
  
  
}