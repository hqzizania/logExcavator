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

import java.io.{PrintWriter, File}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vectors, Matrix, Vector}
import org.apache.spark.mllib.feature._

/**
 *  Created by qhuang on 7/6/15.
 */


object Excavator {

  def main(args: Array[String]) {
    println("*****************logExcavator*******************")
    if (args.length < 4) {
      System.err.println("Usage: logExcavator <master> <path to log> <number of clusters> <path to test>")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("SparkLogExcavator")
      .setMaster(args(0))
    val sc = new SparkContext(conf)

    
    val source = sc.textFile(args(3)).cache()
    val train: RDD[Seq[String]] = sc.textFile(args(1))
      .map(_.split(" ").toSeq).map(p => p.slice(5, p.length))
    val test: RDD[Seq[String]] = source
      .map(_.split(" ").toSeq).map(p => p.slice(5, p.length))

    val clusters: RDD[Int] = kmeansCluster(train, args(2).toInt, 2000, test)
    val clustersPlusSource: RDD[(Int, String)]= clusters.zip(source).sortByKey().cache()

    logWriter(args(2).toInt, clustersPlusSource)

  }

  def logWriter(nCluster: Int, clustersPlusSource: RDD[(Int, String)]) {
    val file=new File("data/clusters")
    val out=new PrintWriter(file)

    for (k <- 0 until nCluster) {
      val filter = clustersPlusSource.filter(_._1==k).collect()
      for (j <- 0 until filter.length) {
        logMsg(out, filter(j).toString)
      }
    }
  }

  def logMsg(out: PrintWriter, msg: String) {
      out.println(msg)
      out.flush()
//    System.out.println(msg)
  }

  def kmeansCluster(train: RDD[Seq[String]],
      numClusters: Int, numIteration: Int,
      test: RDD[Seq[String]]): RDD[Int] = {
    val hashingTF = new HashingTF(1000)
    val tf: RDD[Vector] = hashingTF.transform(train)
    val tf_test: RDD[Vector] = hashingTF.transform(test)
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf).cache()
    val tfidf_test: RDD[Vector] = idf.transform(tf_test).cache()
/*
 * TODO: PCA limits the number of features of HashingTF (default value: 2^20)
         preventing from OOM during PCA at a less number (about 1000), hashingTF
         limits itself by itself at a litter greater number (about 5000) though.
         Additionally, it is needed to investigate the result when PCA added.
 *
 *  val pca = new PCA(10).fit(tfidf)
 *  val pc = tfidf.map(p => pca.transform(p))
 *  KMeans.train(pc, numClusters, numIteration).predict(pc)
 */
    KMeans.train(tfidf, numClusters, numIteration).predict(tfidf_test)
  }

}


