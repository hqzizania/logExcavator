package org.alitouka.spark.dbscan.spatial

import org.apache.commons.math3.ml.distance.DistanceMeasure

trait DistanceMeasureSuite extends Serializable {
  
  def distanceMeasure:DistanceMeasure
  
  def regionCalculator:Class[_<:RegionCalculator]
  
  def regionPartitioner:Class[_<:RegionPartitionIndex]
  
  def origin(numOfDimensions:Int):Point
  
}