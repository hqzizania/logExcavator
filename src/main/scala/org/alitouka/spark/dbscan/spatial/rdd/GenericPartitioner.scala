package org.alitouka.spark.dbscan.spatial.rdd

import org.apache.spark.Partitioner
import org.alitouka.spark.dbscan.spatial.{PointSortKey, Point}
import org.alitouka.spark.dbscan.RegionId
import org.alitouka.spark.dbscan.spatial.Region

/** A partitioner which assigns each entry in a dataset to a [[org.alitouka.spark.dbscan.spatial.Box]]
  *
  * @param boxes A collection of [[org.alitouka.spark.dbscan.spatial.Box]]es
  */
private [dbscan] class GenericPartitioner (val boxes: Iterable[Region]) extends Partitioner {

  assert (boxes.forall(_.partitionId >= 0))

  private val boxIdsToPartitions = generateBoxIdsToPartitionsMap(boxes)

  override def numPartitions: Int = boxes.size

  def getPartition(key: Any): Int = {

    key match {
      case k: PointSortKey => boxIdsToPartitions(k.regionId)
      case boxId: RegionId => boxIdsToPartitions(boxId)
      case pt: Point => boxIdsToPartitions(pt.regionId)
      case _ => 0 // throw an exception?
    }
  }


  private def generateBoxIdsToPartitionsMap (boxes: Iterable[Region]): Map[RegionId, Int] = {
    boxes.map ( x => (x.regionId, x.partitionId)).toMap
  }
}

private [dbscan] object GenericPartitioner {

  def assignPartitionIdsToBoxes(boxes: Iterable[Region]): Iterable[Region] = {
    boxes.zip (0 until boxes.size).map ( x => x._1.withPartitionId(x._2) )
  }

}