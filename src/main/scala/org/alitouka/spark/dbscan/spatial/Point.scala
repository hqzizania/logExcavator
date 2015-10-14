package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan._

/** Represents a point in multi-dimensional space and metadata required by the distributed DBSCAN algorithm
  *
  * @param originId Point's original ID
  * @param coordinates Point's coordinates
  * @param pointId A unique identifier of the point
  * @param regionId An identifier of a partition of a data set which this point belongs to
  * @param distanceFromOrigin Distance of this point from origin
  * @param precomputedNumberOfNeighbors Number of point's neighbors
  * @param clusterId ID of a cluster which this points belongs to
  */
class Point (

    val coordinates: PointCoordinates,
    val pointId: PointId = 0,
    val regionId: RegionId = 0,
    val distanceFromOrigin: Double = 0.0,
    val precomputedNumberOfNeighbors: Long = 0,
    val clusterId: ClusterId = DbscanModel.UndefinedCluster,
    val originId: Long = 0) extends Serializable with Ordered[Point] {

  def this (coords: Array[Double]) = this (new PointCoordinates (coords))

  def this (pt: Point) = this (pt.coordinates, pt.pointId, pt.regionId, pt.distanceFromOrigin,
      pt.precomputedNumberOfNeighbors,  pt.clusterId, pt.originId)

  def this (coords: Double*) = this (new PointCoordinates (coords.toArray))

  def withOriginId (newOId: Long) = {
    new Point (this.coordinates, this.pointId, this.regionId, this.distanceFromOrigin,
      this.precomputedNumberOfNeighbors,  this.clusterId, newOId)
  }

  def withPointId (newId: PointId) = {
    new Point (this.coordinates, newId, this.regionId, this.distanceFromOrigin,
        this.precomputedNumberOfNeighbors,  this.clusterId, this.originId)
  }

  def withBoxId (newBoxId: RegionId) = {
    new Point (this.coordinates, this.pointId, newBoxId, this.distanceFromOrigin,
        this.precomputedNumberOfNeighbors,  this.clusterId, this.originId)
  }

  def withDistanceFromOrigin (newDistance: Double) = {
    new Point (this.coordinates, this.pointId, this.regionId, newDistance,
        this.precomputedNumberOfNeighbors,  this.clusterId, this.originId)
  }

  def withNumberOfNeighbors (newNumber: Long) = {
    new Point (this.coordinates, this.pointId, this.regionId, this.distanceFromOrigin, newNumber,
       this.clusterId, this.originId)
  }

  def withClusterId (newId: ClusterId) = {
    new Point (this.coordinates, this.pointId, this.regionId, this.distanceFromOrigin, this.precomputedNumberOfNeighbors,
      newId, this.originId)
  }

  override def equals (that: Any): Boolean = {

    if (that.isInstanceOf[Point]) {
      that.asInstanceOf[Point].canEqual(this) &&
        this.coordinates == that.asInstanceOf[Point].coordinates // We take only coordinates into account
                                                                 // and don't care about other attributes
    }
    else {
      false
    }
  }

  override def hashCode (): Int = {
    coordinates.hashCode() // We take only coordinates into account
                           // and don't care about other attributes
  }

  override def toString (): String = {
    "Point at (" + coordinates.mkString(", ") + "); id = " + pointId + "; region = " + regionId +
      "; cluster = " + clusterId + "; neighbors = " + precomputedNumberOfNeighbors + "; originId = " + originId
  }

  def canEqual(other: Any) = other.isInstanceOf[Point]

  override def compare(that: Point): Int = {
    var result = 0
    var i = 0

    while (result == 0 && i < coordinates.size) {
      result = this.coordinates(i).compareTo(that.coordinates(i))
      i += 1
    }

    result
  }
}
