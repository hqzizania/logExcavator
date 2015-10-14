package org.alitouka.spark.dbscan.util.commandLine

import org.alitouka.spark.dbscan.spatial.DistanceMeasureSuite

private [dbscan] class CommonArgsParser [C <: CommonArgs] (val args: C, programName: String)
  extends scopt.OptionParser[Unit] (programName) {

  opt[String] ("ds-master")
    .foreach { args.masterUrl = _ }
    .required ()
    .valueName ("<url>")
    .text ("Master URL")

  opt[String] ("ds-jar")
    .foreach { args.jar = _ }
    .required ()
    .valueName ("<jar>")
    .text ("Path to dbscan_prototype.jar which is visible to all nodes in your cluster")

  opt[String] ("ds-input")
    .foreach { args.inputPath = _ }
    .required()
    .valueName("<path>")
    .text("Input path")

  opt[String] ("ds-output")
    .foreach { args.outputPath = _ }
    .required()
    .valueName("<path>").text("Output path")

  opt[String] ("distanceMeasureSuite").foreach {
    x => args.distanceMeasureSuite = Class.forName(x).newInstance().asInstanceOf[DistanceMeasureSuite]
  }

  opt[String] ("ds-debugOutput").foreach { x => args.debugOutputPath = Some(x) }

}
