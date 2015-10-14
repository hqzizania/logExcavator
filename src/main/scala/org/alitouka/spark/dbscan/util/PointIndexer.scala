package org.alitouka.spark.dbscan.util

import java.lang.Math
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.alitouka.spark.dbscan._
import scala.Some
import org.alitouka.spark.dbscan.spatial.Region
import org.alitouka.spark.dbscan.spatial.DistanceMeasureSuite

private [dbscan] class PointIndexer (val numberOfPartitions: Int, val currentPartition: Int) {

  val multiplier = computeMultiplier (numberOfPartitions)
  var currentIndex = 0

  def getNextIndex = {
    currentIndex += 1
    currentIndex * multiplier + currentPartition
  }

  def computeMultiplier (numberOfPartitions: Int) = {
    val numberOfDigits = Math.floor (java.lang.Math.log10 (numberOfPartitions)) + 1

    Math.round (Math.pow (10, numberOfDigits))
  }

}

private [dbscan] object PointIndexer {

  def addMetadataToPoints (
      data: RawDataSet,
      boxes: Broadcast[Iterable[Region]],
      dimensions: Broadcast[Int],
      distanceMeasureSuite: DistanceMeasureSuite): RDD[(PointSortKey, Point)] = {

    val numPartitions = data.partitions.length
    val origin: Point = distanceMeasureSuite.origin(dimensions.value)

    data.mapPartitionsWithIndex( (partitionIndex, points) => {

      val pointIndexer = new PointIndexer (numPartitions, partitionIndex)

      points.map (pt => {

        val pointIndex = pointIndexer.getNextIndex
        val box = boxes.value.find( _.isPointWithin(pt) )
        val distanceFromOrigin = distanceMeasureSuite.distanceMeasure.compute(pt.coordinates.toArray, origin.coordinates.toArray)
        val boxId = box match {
          case existingBox: Some[Region] => existingBox.get.regionId
          case _ => 0 // throw an exception?
        }

        val newPoint = new Point (pt.coordinates, pointIndex, boxId, distanceFromOrigin,
            pt.precomputedNumberOfNeighbors, pt.clusterId, pt.originId)

        (new PointSortKey (newPoint), newPoint)
      })
    })

  }
}
