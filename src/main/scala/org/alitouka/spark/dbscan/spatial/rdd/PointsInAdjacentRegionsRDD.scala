package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}
import org.alitouka.spark.dbscan.PairOfAdjacentRegionIds
import org.alitouka.spark.dbscan.spatial.RegionCalculator
import org.alitouka.spark.dbscan.spatial.Region


/**
 * An RDD that stores points which are close to each other but
 * reside in different density-based partitions of the original data set
 * (these partitions are referred to as "boxes" below)
 *
 * Each partition of this RDD contains points in 2 adjacent boxes
 * Each point may appear multiple times in this RDD - as many times as many boxes are adjacent to the box in which
 * this point resides
 *
 * @param prev An RDD where each entry contains a pair of IDs of adjacent boxes and a point which resides in one of
 *             these boxes
 * @param adjacentBoxIdPairs A collection of distinct pairs of box IDs
 */
private [dbscan] class PointsInAdjacentRegionsRDD (prev: RDD[(PairOfAdjacentRegionIds, Point)], val adjacentBoxIdPairs: Array[PairOfAdjacentRegionIds])
  extends ShuffledRDD [PairOfAdjacentRegionIds, Point, Point] (prev, new AdjacentRegionsPartitioner(adjacentBoxIdPairs))

private [dbscan] object PointsInAdjacentRegionsRDD {

  def apply (points: RDD[Point], boxesWithAdjacentBoxes: Iterable[Region]): PointsInAdjacentRegionsRDD = {
    val adjacentBoxIdPairs = RegionCalculator.generateDistinctPairsOfAdjacentRegionIds(boxesWithAdjacentBoxes).toArray

    val broadcastBoxIdPairs = points.sparkContext.broadcast(adjacentBoxIdPairs)

    val pointsKeyedByPairOfBoxes = points.mapPartitions {
      it => {

        val boxIdPairs = broadcastBoxIdPairs.value

        for (p <- it; pair <- boxIdPairs; if p.regionId == pair._1 || p.regionId == pair._2)
          yield (pair, p)
      }
    }

    new PointsInAdjacentRegionsRDD(pointsKeyedByPairOfBoxes, adjacentBoxIdPairs)
  }
}