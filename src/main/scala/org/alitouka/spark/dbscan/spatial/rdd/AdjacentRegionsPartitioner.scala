package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.Partitioner
import org.alitouka.spark.dbscan.{PairOfAdjacentRegionIds, RegionId}
import org.alitouka.spark.dbscan.spatial.RegionCalculator
import org.alitouka.spark.dbscan.spatial.Region


/** Partitions an [[org.alitouka.spark.dbscan.spatial.rdd.PointsInAdjacentBoxesRDD]] so that each partition
  * contains points which reside in two adjacent boxes
 *
 * @param adjacentBoxIdPairs
 */
private [dbscan] class AdjacentRegionsPartitioner (private val adjacentBoxIdPairs: Array[PairOfAdjacentRegionIds]) extends Partitioner {

  def this (boxesWithAdjacentBoxes: Iterable[Region]) = this (RegionCalculator.generateDistinctPairsOfAdjacentRegionIds(boxesWithAdjacentBoxes).toArray)

  override def numPartitions: Int = adjacentBoxIdPairs.length

  override def getPartition(key: Any): Int = {
    key match {
      case (b1: RegionId, b2: RegionId) => adjacentBoxIdPairs.indexOf((b1, b2))
      case _ => 0 // Throw an exception?
    }
  }
}
