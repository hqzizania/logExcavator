package org.alitouka.spark.dbscan.spatial

private [dbscan] class PointSortKey (pt: Point) extends  Ordered[PointSortKey] with Serializable {
  val regionId = pt.regionId
  val pointId = pt.pointId
  val originId = pt.originId

  override def compare(that: PointSortKey): Int = {

    if (this.regionId > that.regionId) {
      1
    }
    else if (this.regionId < that.regionId) {
      -1
    }
    else if (this.pointId > that.pointId) {
      1
    }
    else if (this.pointId < that.pointId) {
      -1
    }
    else {
      0
    }
  }

  override def equals (that: Any): Boolean = {

    if (that.isInstanceOf[PointSortKey]) {
      that.asInstanceOf[PointSortKey].canEqual(this) &&
      this.pointId == that.asInstanceOf[PointSortKey].pointId &&
      this.regionId == that.asInstanceOf[PointSortKey].regionId
    }
    else {
      false
    }
  }

  override def hashCode (): Int = {
    41 * (41 * pointId.toInt) + regionId
  }

  def canEqual(other: Any) = other.isInstanceOf[PointSortKey]

  override def toString (): String = {
    s"PointSortKey with box: $regionId , ptId: $pointId"
  }
}
