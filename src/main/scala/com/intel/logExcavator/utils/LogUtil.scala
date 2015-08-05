package com.intel.logExcavator.utils

import java.io.{PrintWriter, File}
import org.apache.spark.rdd.RDD
import com.intel.logExcavator.utils._


/**
 * Created by qhuang on 7/21/15.
 */
object LogUtil {

  def logTopics(path: String, topics: Array[Array[(String, Double)]], params: Params) {
    val file=new File(path)
    val out=new PrintWriter(file)
    logMsg(out, s"${params.k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      logMsg(out, s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        logMsg(out, s"$term\t$weight")
      }
      logMsg(out, "\n")
    }
  }

  def logCorpus(path: String, actualCorpusSize: String, actualVocabSize: String, actualNumTokens: String) {
    val file=new File(path)
    val out=new PrintWriter(file)
    logMsg(out, s"Corpus summary:")
    logMsg(out, s"\t Training set size: $actualCorpusSize documents")
    logMsg(out, s"\t Vocabulary size: $actualVocabSize terms")
    logMsg(out, s"\t Training set size: $actualNumTokens tokens")
    logMsg(out, "\n")
  }


  def logMsg(out: PrintWriter, msg: String) {
    out.println(msg)
    out.flush()
    //    System.out.println(msg)
  }


}
