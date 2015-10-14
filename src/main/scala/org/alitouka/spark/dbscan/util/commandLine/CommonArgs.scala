package org.alitouka.spark.dbscan.util.commandLine

import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.spatial.DistanceMeasureSuite

private [dbscan] class CommonArgs (
  var masterUrl: String = null,
  var jar: String = null,
  var inputPath: String = null,
  var outputPath: String = null,
  var distanceMeasureSuite: DistanceMeasureSuite = DbscanSettings.getDefaultDistanceMeasureSuite,
  var debugOutputPath: Option[String] = None)
