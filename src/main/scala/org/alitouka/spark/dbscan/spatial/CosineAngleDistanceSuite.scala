package org.alitouka.spark.dbscan.spatial

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan.util.math.CosineDistance

class CosineAngleDistanceSuite extends DistanceMeasureSuite {
  val dm = new CosineDistance
  
  def distanceMeasure: DistanceMeasure = dm

  def origin(numOfDimensions: Int): Point = {
    val coords=Array.fill(numOfDimensions)(0.0)
    coords(0)=1.0
    new Point(coords)
  }

  def regionCalculator: Class[_ <: org.alitouka.spark.dbscan.spatial.RegionCalculator] = 
    classOf[org.alitouka.spark.dbscan.spatial.sector.HyperSectorCalculator]
  

  def regionPartitioner: Class[_ <: org.alitouka.spark.dbscan.spatial.RegionPartitionIndex] = 
    classOf[org.alitouka.spark.dbscan.spatial.sector.HyperSectorPartitionIndexer]
    
    
}