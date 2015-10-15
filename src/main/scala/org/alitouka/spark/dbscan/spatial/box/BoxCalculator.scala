package org.alitouka.spark.dbscan.spatial.box

import org.alitouka.spark.dbscan.{PairOfAdjacentRegionIds, RegionId, DbscanSettings, RawDataSet}
import org.alitouka.spark.dbscan.spatial.rdd.{GenericPartitioner, PartitioningSettings}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.spatial.BoundsInOneDimension
import org.alitouka.spark.dbscan.spatial.RegionCalculator
import org.alitouka.spark.dbscan.spatial.Region

/** Calculates box-shaped regions for density-based partitioning (see [[org.alitouka.spark.dbscan.spatial.rdd.BoxPartitioner]] )
  * and for fast lookup of point's neighbors (see [[org.alitouka.spark.dbscan.spatial.PartitionIndex]]
  *
  * @param data A raw data set
  */
private [dbscan] class BoxCalculator (data: RawDataSet) extends RegionCalculator(data){

  val numberOfDimensions: Int = getNumberOfDimensions (data)

  override def generateDensityBasedPartitions(partitioningSettings: PartitioningSettings = new PartitioningSettings (),
                                 dbscanSettings: DbscanSettings = new DbscanSettings ()): (Iterable[Region], Region) = {

    val datasetBounds: List[BoundsInOneDimension] = calculateBounds(data, numberOfDimensions)
    val rootBox = new Box (datasetBounds.toArray)
    val sampleData: Array[Point] = data.takeSample(true, 100, new java.text.SimpleDateFormat("ss").format(new java.util.Date()).toLong)
    val boxTree = RegionCalculator.generateTreeOfRegions(rootBox.asInstanceOf[Region], partitioningSettings, dbscanSettings, sampleData)

    val broadcastBoxTree = data.sparkContext.broadcast(boxTree)

    val partialCounts: RDD[(RegionId, Long)] = data.mapPartitions {
      it => {
        val bt = broadcastBoxTree.value.clone ()
        RegionCalculator.countPointsInOneRegion(bt, it)
      }
    }

    val totalCounts = partialCounts.foldByKey(0)(_+_).collectAsMap()
    val boxesWithEnoughPoints = boxTree.flattenBoxes {
      x => totalCounts (x.region.regionId) >= partitioningSettings.numberOfPointsInBox
    }

    RegionCalculator.assignAdjacentRegions (boxesWithEnoughPoints)

    (GenericPartitioner.assignPartitionIdsToBoxes(boxesWithEnoughPoints), rootBox.asInstanceOf[Box])
  }




  private [dbscan] def getNumberOfDimensions (ds: RawDataSet): Int = {
    val pt = ds.first()
    pt.coordinates.length
  }

  def calculateBoundingBox: Region = new Box (calculateBounds (data, numberOfDimensions).toArray)

  private [dbscan] def calculateBounds (ds: RawDataSet, dimensions: Int): List[BoundsInOneDimension] = {
    val minPoint = new Point (Array.fill (dimensions)(Double.MaxValue))
    val maxPoint = new Point (Array.fill (dimensions)(Double.MinValue))

    val mins = fold (ds, minPoint, x => Math.min (x._1, x._2))
    val maxs = fold (ds, maxPoint, x => Math.max (x._1, x._2))

    mins.coordinates.zip (maxs.coordinates).map ( x => new BoundsInOneDimension (x._1, x._2, true) ).toList
  }

  private def fold (ds: RawDataSet, zeroValue: Point, mapFunction: ((Double, Double)) => Double) = {
    ds.fold(zeroValue) {
      (pt1, pt2) => {
        new Point (pt1.coordinates.zip (pt2.coordinates).map ( mapFunction ).toArray)
      }
    }
  }
}
