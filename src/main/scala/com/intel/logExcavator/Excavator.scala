/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.logExcavator

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import scopt.OptionParser
import com.intel.logExcavator.utils._
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.alitouka.spark.dbscan._

import scala.collection.Map
import scala.collection.immutable.Iterable


/**
 *  Created by qhuang on 7/6/15.
 */


object Excavator {



  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LogExcavator") {
      head("*****************logExcavator*******************")
      opt[String]("master")
        .text(s"Spark master address." +
        s" default: ${defaultParams.master}")
        .action((x, c) => c.copy(master = x))
      opt[Int]("slices")
        .text(s"number of slices. default: ${defaultParams.slices}")
        .action((x, c) => c.copy(slices = x))
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[String]("testfile")
        .text(s"file paths (directories) to plain text to test.. " +
        s" Each text file line should hold 1 document." +
        s" default: ${defaultParams.testfile}")
        .action((x, c) => c.copy(testfile = x))
      opt[String]("jobname")
        .text(s"this job name used to create directory in hdfs.. " +
        s" default: ${defaultParams.jobname}")
        .action((x, c) => c.copy(jobname = x))
      opt[String]("hdfs")
        .text(s"hdfs address.." +
        s"  default: ${defaultParams.hdfs}")
        .action((x, c) => c.copy(hdfs = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Double]("docConcentration")
        .text(s"amount of topic smoothing to use (> 1.0) (-1=auto)." +
        s"  default: ${defaultParams.docConcentration}")
        .action((x, c) => c.copy(docConcentration = x))
      opt[Double]("topicConcentration")
        .text(s"amount of term (word) smoothing to use (> 1.0) (-1=auto)." +
        s"  default: ${defaultParams.topicConcentration}")
        .action((x, c) => c.copy(topicConcentration = x))
      opt[Int]("vocabSize")
        .text(s"number of distinct word types to use, chosen by frequency. (-1=all)" +
        s"  default: ${defaultParams.vocabSize}")
        .action((x, c) => c.copy(vocabSize = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
        s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = x))
      opt[String]("algorithm")
        .text(s"inference algorithm to use. tfidf, emLDA and onlineLDA are supported." +
        s" default: ${defaultParams.algorithm}")
        .action((x, c) => c.copy(algorithm = x))
      opt[String]("clusterMethod")
        .text(s"inference clustering algorithm to use. kmeans and dbscan are supported." +
        s" default: ${defaultParams.clusterMethod}")
        .action((x, c) => c.copy(clusterMethod = x))
      opt[String]("checkpointDir")
        .text(s"Directory for checkpointing intermediate results." +
        s"  Checkpointing helps with recovery and eliminates temporary shuffle files on disk." +
        s"  default: ${defaultParams.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"Iterations between each checkpoint.  Only used if checkpointDir is set." +
        s" default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora." +
        "  Each text file line should hold 1 document.")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = c.input :+ x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }

  }

  private def run(params: Params) {

    val conf = new SparkConf()
      .setAppName("SparkLogExcavator")
      .setMaster(params.master)

    val sc = new SparkContext(conf)

    val resultsDirect = params.hdfs + params.jobname

    val clusterMethod = params.clusterMethod match {
      case "kMeans" => println(s"k-Means algorithm will be used to clustering.")
      case "dbScan" => println(s"DBScan algorithm will be used to clustering.")
      case _ => throw new IllegalArgumentException(
        s"Only kMeans and dbScan clustering algorithms are supported but got ${params.algorithm}.")
    }

    val algorithm = params.algorithm match {
      case "tfidf" => runTFIDF(s"$resultsDirect/clusters-tfidf", params, sc)
      case "word2Vec" => runWord2Vec(s"$resultsDirect/clusters-word2Vec", params, sc)
      case "emLDA" | "onlineLDA" => runLDA(s"$resultsDirect/clusters-lda", params, sc)
      case _ => throw new IllegalArgumentException(
        s"Only tfidf, word2Vec, em and online are supported but got ${params.algorithm}.")
    }
    sc.stop()



  }

  private def runTFIDF(path: String, params: Params, sc: SparkContext) {
    val source = sc.textFile(params.input.mkString(","), params.slices).filter(_.size > 50).cache()
    val regex = "\\d+".r
    val regex2= ".*[a-zA-Z].*".r
    val train: RDD[Seq[String]] = source
      .map(_.split("\\s+")).map(p => p.slice(4, p.length))//.filterNot(x => regex.pattern.matcher(x).matches).filter(x => regex2.pattern.matcher(x).matches))


//    val tokenizer = new SimpleTokenizer(sc, params.stopwordFile)
//    val train: RDD[Seq[String]] = train2.map{ text => tokenizer.getWords(text.toString).toSeq}
//    val trainC = train.collect()
    val test: RDD[Seq[String]] = train
    if (params.testfile.nonEmpty) {
      val test: RDD[Seq[String]] = sc.textFile(params.testfile, params.slices)
        .map(_.split("\\s+").toSeq).map(p => p.slice(4, p.length).filterNot(x => regex.pattern.matcher(x).matches))
    }
//    val trainC = train.collect()
//    val testC = test.collect()
    val kCluster = new tfidfCluster()
    val clusters: RDD[Int] = kCluster.tfidfCluster(train, params.k, 20, test, params, sc)
//    val clustersC = clusters.collect()
    val clustersPlusSource: RDD[(Int, String)]= clusters.zipWithIndex().map(_.swap).join(source.zipWithIndex().map(_.swap)).map(_._2).sortByKey()
//    val clustersPlusSourceC = clustersPlusSource.collect()
//    SaveAsFile.saveAsTSFile(path, clustersPlusSource, sc, params.stopwordFile)
    SaveAsFile.saveAsTextFileInt(path, clustersPlusSource)


  }

  private def runWord2Vec(path: String, params: Params, sc: SparkContext) {
    val source = sc.textFile(params.input.mkString(","), params.slices).filter(_.size > 50).cache()


    val tokenizer = new SimpleTokenizer(sc, params.stopwordFile)
    val tokenized: RDD[IndexedSeq[String]] = source.map { text => tokenizer.getWords(text.toString)
      //      id -> text.filterNot(x => regex.pattern.matcher(x).matches)
      //                .filter(x => regex2.findAllIn(x).mkString.length != 0).filter(_.size < 10).filter(_.size > 3)
    }.cache()



    val word2vec = new Word2Vec()

    val model = word2vec.fit(tokenized)

    def Vec(x: Seq[String]): Vector = {

      val wordList: Iterable[String] = model.getVectors.map{case (word, ind) => word}
      var a: Array[Double] = model.transform(wordList.toList(0)).toArray
      for(i <- x) {
        if(wordList.exists(s => s == i)) {
          val b = (model.transform(i))
          for(j <- 0 until b.size) {
            a(j) = a(j) + b(j)
          }
        }
      }
      Vectors.dense(a)
    }

    val vecs: RDD[Vector] = tokenized.map(p => Vec(p)).cache()


    val clusters: RDD[Int] = KMeans.train(vecs, params.k, 20).predict(vecs)

    val clustersPlusSource: RDD[(Int, String)]= clusters.zip(source).sortByKey()

    SaveAsFile.saveAsTSFile(path, clustersPlusSource, sc, params.stopwordFile)
  }

  private def runLDA(path: String, params: Params, sc: SparkContext): Unit = {
    // Load documents, and prepare them for LDA.
    val (corpus, vocabArray, actualNumTokens) =
      Preprocess.preprocess(sc, params.input, params.vocabSize, params.stopwordFile, params.slices)
    corpus.cache()
    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.size


    LogUtil.logCorpus("results/corpusSummary", actualCorpusSize.toString, actualVocabSize.toString, actualNumTokens.toString)

    // Run LDA.
    val lda = new LDA()

    val optimizer = params.algorithm match {
      case "emLDA" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "onlineLDA" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
    }

    lda.setOptimizer(optimizer)
      .setK(params.k)
      .setCheckpointInterval(10)
    if (params.checkpointDir.nonEmpty) {
      sc.setCheckpointDir(params.checkpointDir.get)
    }

    val ldaModel = lda.run(corpus)
    println(s"Finished training LDA model. Summary:")

    val source = sc.textFile(params.input.mkString(","), params.slices).filter(_.size > 50).cache()

    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
      val dist: RDD[(Long, Int)] = distLDAModel.topicDistributions.map { case (docID, topicCounts) =>
        (docID, pick(topicCounts))
      }.sortByKey()
      val clustersPlusSource: RDD[(Int, String)]= dist.
        join(source.zipWithIndex().map(_.swap)).map(_._2).sortByKey()

      SaveAsFile.saveAsTSFile(path, clustersPlusSource, sc, params.stopwordFile)
    }
    else {
      // score method:
      def score(line: Vector, topicIndices : Array[(Array[Int], Array[Double])] ) = {
        var w = new Array[Double](topicIndices.size)
        for(i <- topicIndices) {
          for( j <- i._1) {
            w(topicIndices.indexOf(i)) += line(j) * i._2(i._1.indexOf(j))
          }
        }
        w.indexOf(w.max)
      }
      val topicsMatrix: DenseMatrix = new DenseMatrix(actualVocabSize, params.k, ldaModel.topicsMatrix.toArray)
      val topicsMatrixBC = sc.broadcast(topicsMatrix)
      val corpusScore = corpus.map{case (id, terms) => (id, Matrices.dense(1, actualVocabSize, terms.toArray).multiply(topicsMatrixBC.value))}
                              .map{case (id, terms) => (id, terms.toArray.indexOf(terms.toArray.max))}

      val clustersPlusSource: RDD[(Int, String)]= corpusScore
        .join(source.zipWithIndex().map(_.swap)).map(_._2).sortByKey()
      source.unpersist()

      SaveAsFile.saveAsTSFile(path, clustersPlusSource, sc, params.stopwordFile)


    }


    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = actualVocabSize)
    val topics: Array[Array[(String, Double)]] = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }

    LogUtil.logTopics("results/describeTopics", topics, params)


  }

  private def pick(topicCounts: Vector): Int = {
    topicCounts.toArray.indexOf(topicCounts.toArray.max)
  }

}


