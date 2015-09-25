package com.intel.logExcavator.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by qhuang on 9/22/15.
 */
object SaveAsFile {
  def saveAsTextFile(path: String, clustersPlusSource: RDD[(String, String)]): Unit = {
    clustersPlusSource.saveAsTextFile(path)
  }
  def saveAsTextFileInt(path: String, clustersPlusSource: RDD[(Int, String)]): Unit = {
    clustersPlusSource.saveAsTextFile(path)
  }
  def saveAsTSFile(path: String, clustersPlusSource: RDD[(Int, String)], sc: SparkContext, stopwordFile: String): Unit = {

    val yearFormat = new java.text.SimpleDateFormat("yyyy")
    val year = yearFormat.format(new java.util.Date())
    val timeFormat = new java.text.SimpleDateFormat("dd-MMM-yyyy:HH:mm:ss")
    val keys = clustersPlusSource.groupByKey().keys.collect()
    val TSInt: RDD[(Int, String)] = clustersPlusSource.map{ case (k, v) => (keys.indexOf(k), v) }.sortByKey()

    var TSString: RDD[(String, String)] = TSInt.map{p => (p._1.toString(), p._2)}
    val regex = "\\d+".r
    val regex2= ".*[a-zA-Z].*".r
    val seed = new java.text.SimpleDateFormat("ss").format(new java.util.Date()).toLong
    for( i <- keys ) {
      val sample = TSInt.filter(_._1 == i).takeSample(false, 5, seed)
      var sampleA: Array[String] = sample.map(_._2).map(_.split("\\s+")).map(p => p.slice(4, p.length).filterNot(x => regex.pattern.matcher(x).matches).filter(x => regex2.pattern.matcher(x).matches))
          .reduceLeft(_.intersect(_)).take(20)
        //.groupBy(identity).toArray.sortBy(-_._2.size).take(5)
      if(sampleA.size < 1) sampleA = Array("unidentified", "group")

      val tokenizer = new SimpleTokenizer(sc, stopwordFile)
      val tokenized: String = sampleA.flatMap{ text => tokenizer.getWords(text.toString)}.mkString("_")
      TSString = TSString.map{case(k, v) => if(k == i.toString) (tokenized, v) else (k.toString, v)}
    }
    saveAsTextFile(path + "/origin", TSString)
    val TS = TSString
      .map{ case (k, v) => {
        val p: Seq[String] = v.split("\\s+").toSeq
        def Time(p : Seq[String]) = {
          timeFormat.parse((p(1) + "-" + p(0) + "-" + year + ":" + p(2))).getTime()/1000
        }
        (k, Time(p), 1.toString, "ip=" + p(3), "log=" + v)
      }}.sortBy(_._2).zipWithIndex()

    TS.map(p => "logExca.clustering " + p._1._2 + " " + p._1._3 + " clusterId=" + p._1._1 + " " + p._1._4 + " uniqueId=" + p._2).saveAsTextFile(path + "/TS")
    TS.map(p => "logExca.clustering " + p._1._2 + " " + p._1._3 + " clusterId=" + p._1._1 + " " + p._1._4 + " uniqueId=" + p._2 + " " + p._1._5).saveAsTextFile(path + "/TSPlusLog")
  }
}
