package org.alitouka.spark.dbscan.util.math


private [dbscan] class DoubleArrayComparisonOperations (val originalValue: Array[Double]) {

  def ~~ (that: Array[Double]): Boolean = {
    if(that.length!=originalValue.length)
      return false;
    var res=true
    for(i <- 0 to originalValue.length-1){
      res&=isAlmostEqual(originalValue(i), that(i))
    }
    res
  }

  

  private def isAlmostEqual (x: Double, y: Double): Boolean = {
    Math.abs (x - y) <= DoubleComparisonOperations.Eps
  }
}

private [dbscan] object DoubleArrayComparisonOperations {

  val Eps: Double = 1E-10

  implicit def doubleArrayToDoubleArrayComparisonOperations (x: Array[Double]): DoubleArrayComparisonOperations = {
    new DoubleArrayComparisonOperations (x)
  }
}
