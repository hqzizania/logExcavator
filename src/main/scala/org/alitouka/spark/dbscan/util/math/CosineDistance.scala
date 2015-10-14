package org.alitouka.spark.dbscan.util.math

import org.apache.commons.math3.ml.distance.DistanceMeasure
/**
 * A class which computes the distance between two points -
 * represented as arrays of [[scala.Double]] - as 1 minus
 * their cosine similarity value.
 * 
 * This class implements [[org.apache.commons.math3.ml.distance.DistanceMeasure]].
 */
class CosineDistance extends DistanceMeasure{
  
  override def compute(x1:Array[Double], x2:Array[Double]) : Double = {
    
    //sanity checks
    assert(x1.length == x2.length)
    
    val norm1 = CosineDistance.norm(x1)
    val norm2 = CosineDistance.norm(x2)
    
    if(norm1 == 0.0 || norm2 == 0.0 ){
      return Double.MaxValue
    }
    var result:Double = 0.0
    for(i <- 0 to x1.length-1){
      result+=x1(i)*x2(i)
    }
    result = result/norm1/norm2
    if(result>1) result=1 //this may happen due to precision errors
    Math.acos(result)
    
  }
  
  
  
}

object CosineDistance {
  
  def norm(x:Array[Double]) : Double = {
    var result:Double = 0.0
    for(i <- 0 to x.length-1){
      result+=x(i)*x(i)
    }
    Math.sqrt(result)
  }
  
}