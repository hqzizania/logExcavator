package com.intel.logExcavator

import com.intel.logExcavator.utils.Params
import org.alitouka.spark.dbscan._
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.spatial.Point
import org.alitouka.spark.dbscan.util.io.IOHelper
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._


/**
 * Created by qhuang on 7/21/15.
 */
class tfidfCluster extends Serializable{

  def tfidfCluster(train: RDD[Seq[String]],
                    numClusters: Int, numIteration: Int,
                    test: RDD[Seq[String]], params:Params, sc:SparkContext): RDD[Int] = {
    val hashingTF = new HashingTF(10)
    val tf: RDD[Vector] = hashingTF.transform(train)
    val tf_test: RDD[Vector] = hashingTF.transform(test)
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf).cache()
    val tfidf_test: RDD[Vector] = idf.transform(tf_test).cache()

    val tfidf2: RDD[(Long, Vector)] = tfidf.zipWithIndex().map(_.swap)
    val num = tfidf2.count.toInt
    var tfidfA: RDD[(Long, (Vector, Array[Double]))] = tfidf2.map{
      case (k, v) =>
        (k, (v, new Array[Double](num.toInt)))
    }

    if(params.clusterMethod == "dbScanDirect") {
      val broadcastNum = 100
      for (i <- 0 to num.toInt / broadcastNum) {
        val factor: Array[(Long, Vector)] = tfidf2.filter { case (k, v) => List.range(i * broadcastNum, (i + 1) * broadcastNum).contains(k)}.collect()
        val factorSC = sc.broadcast(factor)
        tfidfA = tfidfA.mapPartitions(computeDistPartitions(_, factorSC.value))
      }

      def computeDistPartitions(a: Iterator[(Long, (Vector, Array[Double]))], b: Array[(Long, Vector)]) = {

        var c = List[(Long, (Vector, Array[Double]))]()
        while (a.hasNext) {
          var aa = a.next
          for (i <- b) {
              aa._2._2(i._1.toInt) = computeDist2bit(aa._2._1, i._2)
          }
          c ::= aa
        }
        c.toIterator
      }

      def computeDist2bit(v1: Vector, v2: Vector) = {

        (v1, v2) match {
          case (sx: SparseVector, sy: SparseVector) =>
            dist(sx, sy)
          case _ =>
            throw new IllegalArgumentException(s"dot doesn't support (${v1.getClass}, ${v2.getClass}).")
        }
      }

      def dist(x: SparseVector, y: SparseVector) = {
        val xValues = x.values
        val xIndices: Array[Int] = x.indices
        val yValues = y.values
        val yIndices = y.indices
        val nnzx = xIndices.size
        val nnzy = yIndices.size

        var kx = 0
        var ky = 0
        var sum = 0.0

        for(i <- 0 until nnzx)
            sum += xValues(i) * xValues(i)
        for(i <- 0 until nnzy)
            sum += yValues(i) * yValues(i)

        while (kx < nnzx && ky < nnzy) {
          val ix = xIndices(kx)
          while (ky < nnzy && yIndices(ky) < ix) {
            ky += 1
          }
          if (ky < nnzy && yIndices(ky) == ix) {
            val tmp = xValues(kx) - yValues(ky)
            sum += tmp * tmp - xValues(kx) * xValues(kx) - yValues(ky) * yValues(ky)
            ky += 1
          }
          kx += 1
        }
        sum
      }


      val eps = 20
      val neighborTable: Array[Array[Boolean]] = tfidfA.map {
        case (k, (v, d)) => d.map(p => if (p < eps) true else false)
      }.collect()

      println("beginning dbScan ... ")
      var cluster = new Array[Int](neighborTable.size)
      var active = new Array[Boolean](neighborTable.size)
      var clusterNow = 1
      for (i <- 0 until neighborTable.size) {
        if (!active(i)) {
          active(i) = true
          cluster(i) = clusterNow
          for (j <- 0 until neighborTable(i).size) {
            if (neighborTable(i)(j))
              if (!active(j)) {
                active(j) = true
                cluster(j) = clusterNow
              }
          }
          clusterNow = 1 + clusterNow
        }
      }

      sc.parallelize(cluster,params.slices)
    }

//    var tfidfB= tfidfA
//    for(i <- 0 until num.toInt) {
//      tfidfA = tfidfA.join(tfidfB)
//        .map {
//        case (k, ((v1, d1),(v2, d2))) =>
//          (k, (v1, computeDist(v1.toArray, v2.toArray, d1, i)))
//      }
//      tfidfB = tfidfB.map{case(k, v) => if(k == num-1) (0.toLong, v) else (k+1, v)}
//      val x = tfidfA.collect()
//      val y = tfidfB.collect()
//      println()
//    }
//
//    def computeDist(v1:Array[Double], v2:Array[Double], d:Array[Double], num:Long): Array[Double] = {
//      var bbb = 0.0
//      for (i <- 0 until v1.size) {
//       bbb = bbb + (v1(i) - v2(i)) * (v1(i) - v2(i))
//      }
//      d(num.toInt) = bbb
//      d
//    }
//
//    val x = tfidfA.map{case(k,(v,d)) => d}
//      .collect()
//    println()


    else if (params.clusterMethod == "kMeans")
      KMeans.train(tfidf, numClusters, numIteration).predict(tfidf_test)
    else {
      val tfidfPoints: RDD[Point] = tfidf.zipWithIndex().map{ case (point, id) => new Point(point.toArray).withOriginId(id) }
      val tfidf_testPoints: RDD[Point] = tfidf_test.map(p => new Point(p.toArray))

      val clusteringSettings = new DbscanSettings().withNumberOfPoints(1).withEpsilon(0.1)//.withDistanceMeasureSuite(new CosineAngleDistanceSuite())
      val partitioningSettings: PartitioningSettings = new PartitioningSettings().withNumberOfLevels(10).withNumberOfPointsInBox(100)
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
