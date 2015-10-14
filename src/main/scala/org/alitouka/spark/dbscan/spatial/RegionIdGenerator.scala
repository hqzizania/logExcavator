package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan._

private [dbscan] class RegionIdGenerator (val initialId: RegionId) {
  var nextId = initialId

  def getNextId (): RegionId = {
    nextId += 1
    nextId
  }
}
