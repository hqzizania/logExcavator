package org.alitouka.spark.dbscan.spatial.rdd

import org.alitouka.spark.dbscan.spatial.box.BoxCalculator
import org.alitouka.spark.dbscan.spatial.{Region, RegionCalculator, PointSortKey, Point}
import org.alitouka.spark.dbscan.{PointId, PointCoordinates, RawDataSet, DbscanSettings}
import org.apache.spark.rdd.{RDD}
import org.apache.spark.rdd.ShuffledRDD
import org.alitouka.spark.dbscan.util.PointIndexer
import scala.reflect._

class PointsPartitionedByRegionRDD (prev: RDD[(PointSortKey, Point)],
    val boxes: Iterable[Region], val boundingBox: Region) 
    extends ShuffledRDD[PointSortKey, Point, Point] (prev, new GenericPartitioner(boxes))

object PointsPartitionedByRegionRDD{
  
  
  def apply(rawData: RawDataSet,
    partitioningSettings: PartitioningSettings = new PartitioningSettings (),
    dbscanSettings: DbscanSettings = new DbscanSettings ())
    : PointsPartitionedByRegionRDD = {

    val sc = rawData.sparkContext
    val boxCalculator: RegionCalculator = ClassTag(dbscanSettings.distanceMeasureSuite.regionCalculator).runtimeClass.getConstructors()(0).newInstance(rawData).asInstanceOf[RegionCalculator]
//    val boxCalculator = new BoxCalculator(rawData).asInstanceOf[RegionCalculator]
    val (boxes, boundingBox) = boxCalculator.generateDensityBasedPartitions (partitioningSettings, dbscanSettings)
    val broadcastBoxes = sc.broadcast(boxes)
    var broadcastNumberOfDimensions = sc.broadcast (boxCalculator.numberOfDimensions)

    val pointsInBoxes: RDD[(PointSortKey, Point)] = PointIndexer.addMetadataToPoints(
      rawData,
      broadcastBoxes,
      broadcastNumberOfDimensions,
      dbscanSettings.distanceMeasureSuite)

    PointsPartitionedByRegionRDD (pointsInBoxes, boxes, boundingBox)
  }
  
  def apply (pointsInBoxes: RDD[(PointSortKey, Point)], boxes: Iterable[Region], boundingBox: Region): PointsPartitionedByRegionRDD = {
    new PointsPartitionedByRegionRDD(pointsInBoxes, boxes, boundingBox)
  }


  private [dbscan] def extractPointIdsAndCoordinates (data: RDD[(PointSortKey, Point)]): RDD[(PointId, PointCoordinates)] = {
    data.map ( x => (x._2.pointId, x._2.coordinates) )
  }
  
}