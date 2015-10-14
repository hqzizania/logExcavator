package org.alitouka.spark.dbscan.spatial

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.apache.commons.math3.ml.distance.DistanceMeasure

class EuclideanDistanceSuite extends DistanceMeasureSuite {
  def distanceMeasure: DistanceMeasure = new EuclideanDistance()

  def regionCalculator: 
    Class[_ <: org.alitouka.spark.dbscan.spatial.RegionCalculator] = 
       classOf[org.alitouka.spark.dbscan.spatial.box.BoxCalculator]
  
  def regionPartitioner: 
    Class[_ <: org.alitouka.spark.dbscan.spatial.RegionPartitionIndex] = 
       classOf[org.alitouka.spark.dbscan.spatial.box.BoxPartitionIndex]

  def origin(numOfDimensions: Int): Point = new Point (Array.fill (numOfDimensions)(0.0))
  
  
}