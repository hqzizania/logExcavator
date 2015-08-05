package com.intel.logExcavator

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Matrix, DenseVector, DenseMatrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

/**
 * Created by qhuang on 7/22/15.
 */
object Linealg {

  def computeInverse(X: RowMatrix): DenseMatrix = {
    val nCoef = X.numCols.toInt
    val svd = X.computeSVD(nCoef, computeU = true)
    if (svd.s.size < nCoef) {
      sys.error(s"RowMatrix.computeInverse called on singular matrix.")
    }

    // Create the inv diagonal matrix from S
    val invS = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x,-1))))

    // U cannot be a RowMatrix
    val U = new DenseMatrix(svd.U.numRows().toInt,svd.U.numCols().toInt,svd.U.rows.collect.flatMap(x => x.toArray), false)

    // If you could make V distributed, then this may be better. However its alreadly local...so maybe this is fine.
    val V = svd.V
    // inv(X) = V*inv(S)*transpose(U)  --- the U is already transposed.
    (V.multiply(invS)).multiply(U.transpose)
  }

  def MatrixToRDD(m: Matrix, sc: SparkContext): RDD[Vector] = {
    val columns = m.toArray.grouped(m.numRows)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }

  def ArrayToRDD(m: Int, n: Int, x: Array[Double], sc: SparkContext): RDD[Vector] = {
    val columns = x.grouped(n)
    val rows = columns.toSeq.transpose // Skip this if you want a column-major RDD.
    val vectors = rows.map(row => new DenseVector(row.toArray))
    sc.parallelize(vectors)
  }

}

