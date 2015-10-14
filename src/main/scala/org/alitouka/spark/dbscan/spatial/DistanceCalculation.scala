package org.alitouka.spark.dbscan.spatial

import org.apache.commons.math3.ml.distance.DistanceMeasure
import org.alitouka.spark.dbscan._

private [dbscan] trait DistanceCalculation {

  protected def calculateDistance (pt1: Point, pt2: Point)(implicit distanceMeasure: DistanceMeasure): Double = {
    calculateDistance (pt1.coordinates, pt2.coordinates)
  }

  protected def calculateDistance (pt1: PointCoordinates, pt2: PointCoordinates)
    (implicit distanceMeasure: DistanceMeasure): Double = {

    distanceMeasure.compute (pt1.toArray, pt2.toArray)
  }

  protected def isPointCloseToAnyBound (pt: Point, box: Region, threshold: Double): Boolean = {
    box.isPointCloseToAnyBounds(pt, threshold)
  }

  
}
