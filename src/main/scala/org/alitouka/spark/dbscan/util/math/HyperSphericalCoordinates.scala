package org.alitouka.spark.dbscan.util.math

import org.alitouka.spark.dbscan.util.math.DoubleComparisonOperations._
import org.alitouka.spark.dbscan.util.math.DoubleArrayComparisonOperations._

class HyperSphericalCoordinates (private val radius:Double, private val phis:Array[Double]) extends Serializable {
  
  def R=radius
  def Phis=phis
  
  override def equals(obj:Any):Boolean = {
    if(obj.isInstanceOf[HyperSphericalCoordinates]){
      return (obj.asInstanceOf[HyperSphericalCoordinates].R==this.R 
            && obj.asInstanceOf[HyperSphericalCoordinates].Phis.equals(this.Phis))  
    }
    false
  }
  
  def ~~(that:HyperSphericalCoordinates):Boolean = {
    this.R~~that.R && this.Phis~~that.Phis
  }
  override def toString() : String = {
    var s="R="+R+"; PHI = ["
    for(i <- 0 to phis.length-1) s+=phis(i)+","
    s=s.substring(0, s.length()-1)+"]"
    s
  }
  
}

object HyperSphericalCoordinates{
  /**
   * This method converts the Cartesian coordinates passed as argument
   * into the corresponding spherical coordinates using the formulas 
   * described here [[http://en.wikipedia.org/wiki/N-sphere#Spherical_coordinates]]
   */
  def fromCartesian(cartesian:Array[Double]) : HyperSphericalCoordinates = {
    computeSphericalFromCartesian(cartesian);
  }
  
  private def computeSphericalFromCartesian(cartesian:Array[Double]) : HyperSphericalCoordinates = {
    
    var r:Double = 0
    for(i <- 0 to cartesian.length-1) r+=cartesian(i)*cartesian(i)
    r=Math.sqrt(r)
    
    val phis = new Array[Double](cartesian.length-1)
    
    for( i <- 0 to cartesian.length-2){
      var denom:Double=0
      for(j <- i to cartesian.length-1){
        denom+=cartesian(j)*cartesian(j)
      }
      denom=Math.sqrt(denom)
      if(denom>0)
        phis(i)=Math.acos(cartesian(i)/denom)
       else
        phis(i)=0
      
    }
    if(cartesian(cartesian.length-1)<0){
      phis(cartesian.length-2)=2*Math.PI-phis(cartesian.length-2)
    }
    
    
    new HyperSphericalCoordinates(r,phis)
    
  }
  
  /**
   * This method converts the spherical coordinates passed as argument
   * into the corresponding Cartesian coordinates using the formulas 
   * described here [[http://en.wikipedia.org/wiki/N-sphere#Spherical_coordinates]]
   */
  def fromSphericalToCartesian(spherical:HyperSphericalCoordinates) : Array[Double] = {
    
    val cartesian=new Array[Double](spherical.Phis.length+1)
    for( i <- 0 to cartesian.length-2){
      cartesian(i)=spherical.R
      for(j <- 0 to i-1){
        cartesian(i)*=Math.sin(spherical.Phis(j))
      }
      cartesian(i)*=Math.cos(spherical.Phis(i))
    }
    cartesian(cartesian.length-1)=spherical.R
    for(j <- 0 to cartesian.length-2){
        cartesian(cartesian.length-1)*=Math.sin(spherical.Phis(j))
      }
    cartesian
  }
  
}