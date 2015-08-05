package com.intel.logExcavator

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by qhuang on 7/21/15.
 */
class KmeansCluster extends Serializable{

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
