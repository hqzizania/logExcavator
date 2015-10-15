package com.intel.logExcavator

import com.intel.logExcavator.utils.Params
import org.alitouka.spark.dbscan._
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.spatial.{CosineAngleDistanceSuite, Point}
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Created by qhuang on 7/21/15.
 */
class tfidfCluster extends Serializable{

  def tfidfCluster(train: RDD[Seq[String]],
                    numClusters: Int, numIteration: Int,
                    test: RDD[Seq[String]], params:Params): RDD[Int] = {
    val hashingTF = new HashingTF(10)
    val tf: RDD[Vector] = hashingTF.transform(train)
    val tf_test: RDD[Vector] = hashingTF.transform(test)
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf).cache()
    val tfidf_test: RDD[Vector] = idf.transform(tf_test).cache()

    if (params.clusterMethod == "kMeans")
      KMeans.train(tfidf, numClusters, numIteration).predict(tfidf_test)
    else {
      val tfidfPoints: RDD[Point] = tfidf.zipWithIndex().map{ case (point, id) => new Point(point.toArray).withOriginId(id) }
      val tfidf_testPoints: RDD[Point] = tfidf_test.map(p => new Point(p.toArray))

      val clusteringSettings = new DbscanSettings().withNumberOfPoints(1).withEpsilon(0.1)//.withDistanceMeasureSuite(new CosineAngleDistanceSuite())
      val partitioningSettings: PartitioningSettings = new PartitioningSettings().withNumberOfLevels(100).withNumberOfPointsInBox(10000)//.withNumberOfSplits(100)
      val model: DbscanModel = Dbscan.train(tfidfPoints, clusteringSettings, partitioningSettings)

      model.allPoints.map(_.originId).zip(model.allPoints).sortByKey().map(_._2.clusterId.toInt)
    }


//    IOHelper.saveClusteringResult(model, "results/model")
//    val tfidfPointsC: Array[Point] = tfidfPoints.collect()
//    val tfidfPointsLabel = model.predict(tfidfPointsC(0))

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
    
//
  }

}
