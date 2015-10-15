package org.alitouka.spark.dbscan.spatial

import org.alitouka.spark.dbscan.RawDataSet
import org.alitouka.spark.dbscan.spatial.rdd.PartitioningSettings
import org.alitouka.spark.dbscan.DbscanSettings
import org.alitouka.spark.dbscan.RegionId
import org.alitouka.spark.dbscan.PairOfAdjacentRegionIds
import scala.reflect._

abstract class RegionCalculator(val data:RawDataSet) {
  
  val numberOfDimensions:Int
  
  def generateDensityBasedPartitions(partitioningSettings: PartitioningSettings = new PartitioningSettings (),
                                 dbscanSettings: DbscanSettings = new DbscanSettings ()): (Iterable[Region], Region)
                                 
  def calculateBoundingBox: Region
  
  
}

private [dbscan] object RegionCalculator{
  
  def generateTreeOfRegions(root: Region,
                            partitioningSettings: PartitioningSettings,
                            dbscanSettings: DbscanSettings,
                            sampleData: Array[Point]):TreeItemWithNumberOfPoints = {
    RegionCalculator.generateTreeOfRegions(root, partitioningSettings, dbscanSettings, new RegionIdGenerator(root.regionId), sampleData)
  }


  def generateTreeOfRegions(root: Region,
                           partitioningSettings: PartitioningSettings,
                           dbscanSettings: DbscanSettings,
                           idGenerator: RegionIdGenerator,
                           sampleData: Array[Point]): TreeItemWithNumberOfPoints = {

    val result = new TreeItemWithNumberOfPoints(root)

    result.children = if (partitioningSettings.numberOfLevels > 0 && filterPointsEnough(sampleData, root)) {

      val newPartitioningSettings = partitioningSettings.withNumberOfLevels(partitioningSettings.numberOfLevels-1)

      root
        .splitAlongLongestDimension(partitioningSettings.numberOfSplits, idGenerator)
//        .filter(_.isBigEnough(dbscanSettings))
        .map(x => generateTreeOfRegions(x,
          newPartitioningSettings,
          dbscanSettings,
          idGenerator,
          sampleData))
        .toList
    }
    else {
      List[TreeItemWithNumberOfPoints] ()
    }

    result
  }

  def filterPointsEnough (sampleData: Array[Point], root: Region) = {
    var count = 0
    sampleData.foreach{pt =>
      if (root.bounds.zip (pt.coordinates).forall( x => x._1.isNumberWithin(x._2) ))
        count += 1
    }
    count > 1
  }
  
  def countOnePoint(pt: Point, root: TreeItemWithNumberOfPoints): Unit = {

    if (root.region.isPointWithin(pt)) {
      root.numberOfPoints += 1

      root.children.foreach {
        x => RegionCalculator.countOnePoint(pt, x)
      }
    }
  }
  
  def countPointsInOneRegion (root: TreeItemWithNumberOfPoints, it: Iterator[Point]): Iterator[(RegionId, Long)] = {
    it.foreach (pt => RegionCalculator.countOnePoint (pt, root))
    root.flatten.map {
      x: TreeItemWithNumberOfPoints => { (x.region.regionId, x.numberOfPoints) }
    }.iterator
  }
  
  private [dbscan] def generateCombinationsOfSplits (splits: List[List[BoundsInOneDimension]],
                                                     dimensionIndex: Int): List[List[BoundsInOneDimension]] = {

    if (dimensionIndex < 0) {
      List(List())
    }
    else {
      for {
        i <- RegionCalculator.generateCombinationsOfSplits(splits, dimensionIndex - 1)
        j <- splits(dimensionIndex)
      }
      yield j :: i
    }
  }
  def splitRegionIntoEqualRegions[SHAPE <: Region : ClassTag] (rootBox: SHAPE, maxSplits: Int, dbscanSettings: DbscanSettings) : Iterable[SHAPE] = {

    val dimensions = rootBox.bounds.size
    val splits = rootBox.bounds.map ( _.split(maxSplits, dbscanSettings) )
    val combinations = RegionCalculator.generateCombinationsOfSplits(splits.toList, dimensions-1)
    var constructorIndex = 0
    var currentIndex=0
    for( c <-  classTag.runtimeClass.getConstructors){
      if(c.getParameterTypes.length==2 
          && c.getParameterTypes()(0).getCanonicalName.equals("scala.collection.immutable.List")
          && c.getParameterTypes()(1).getCanonicalName.equals("int")){
        constructorIndex=currentIndex
      }
      currentIndex+=1
    }
    //Console.out.print("Using constructor " + constructorIndex + " -> "+classTag.runtimeClass.getConstructors.apply(constructorIndex))
    for (i <- 0 until combinations.size) yield classTag.runtimeClass.getConstructors.apply(constructorIndex).newInstance(combinations(i).reverse , Int.box(i+1)).asInstanceOf[SHAPE]
    
  }
  
  def computeTreeForPartitionIndex[SHAPE <: Region : ClassTag](rootBox: SHAPE, partitioningSettings: PartitioningSettings, dbscanSettings: DbscanSettings): TreeItemWithPoints = {
    computeTreeLeafs(rootBox, partitioningSettings.numberOfLevelsWithinPartition, partitioningSettings.numberOfSplitsWithinPartition, dbscanSettings)
  }
  def computeTreeLeafs[SHAPE <: Region : ClassTag](rootBox: SHAPE, levels : Int, splitsInLevel: Int, dbscanSettings: DbscanSettings): TreeItemWithPoints = {
    //println("computeTreeLeafs at level "+levels)
    val result = new TreeItemWithPoints(rootBox)

    result.children = if (levels > 0) {

      rootBox
        .splitAlongLongestDimension(splitsInLevel)
//        .filter(_.isBigEnough(dbscanSettings))
        .map(x => computeTreeLeafs(x.asInstanceOf[SHAPE],
          levels-1,
          splitsInLevel,
          dbscanSettings))
        .toList
    }
    else {
      List[TreeItemWithPoints] ()
    }

    result
  }
  
  private [dbscan] def assignAdjacentRegions [SHAPE <: Region] (boxesWithEnoughPoints: Iterable[SHAPE]) (implicit ct:ClassTag[SHAPE]) = {
    
    val temp = boxesWithEnoughPoints.toArray(ct)

    for (i <- 0 until temp.length) {
      for (j <- i+1 until temp.length) {
        if (temp(i).isAdjacentToPartition(temp(j))) {
          temp(i).addAdjacentPartition(temp(j))
          temp(j).addAdjacentPartition(temp(i))
        }
      }
    }
  }

  private [dbscan] def generateDistinctPairsOfAdjacentRegionIds(boxesWithAdjacentBoxes: Iterable[Region]): Iterable[PairOfAdjacentRegionIds] = {

    for (b <- boxesWithAdjacentBoxes; ab <- b.adjacentPartitions; if b.regionId < ab.regionId)
      yield (b.regionId, ab.regionId)

  }

  private [dbscan] def shouldAdjacentRegionBeIncludedInRegion (rootBoxId: RegionId, adjacentBoxId: RegionId): Boolean = {
    rootBoxId <= adjacentBoxId
  }
  
  private [dbscan] def generateEmbracingRegion[SHAPE <: Region : ClassTag](boxes: Iterable[Region]): Region = {

    val it = boxes.iterator
    val firstBox = it.next

    var embracingBoxBounds: Iterable[BoundsInOneDimension] = firstBox.bounds

    it.foreach {
      b => {
        assert (embracingBoxBounds.size == b.bounds.size)

        embracingBoxBounds = embracingBoxBounds.zip (b.bounds).map {
          x => x._1.increaseToFit(x._2)
        }
      }
    }
    var constructorIndex = 0
    var currentIndex=0
    
    for( c <-  classTag.runtimeClass.getConstructors){
      if(c.getParameterTypes.length==4 
          && c.getParameterTypes()(0).getCanonicalName.equals("scala.Array")
          && c.getParameterTypes()(1).getCanonicalName.equals("int")
          && c.getParameterTypes()(2).getCanonicalName.equals("int")
          && c.getParameterTypes()(4).getCanonicalName.equals("scala.collection.immutable.List")){
        constructorIndex=currentIndex
      }
      currentIndex+=1
    }
    //Console.out.println(constructorIndex + " -> "+classTag.runtimeClass.getName + " -> "+classTag.runtimeClass.getConstructors.apply(constructorIndex))
    classTag.runtimeClass.getConstructors.apply(constructorIndex).newInstance(embracingBoxBounds.toArray ,Int.box(0) ,Int.box(-1), Nil).asInstanceOf[Region]
  }


  private [dbscan] def generateEmbracingRegionFromAdjacentRegions[SHAPE <: Region : ClassTag] (rootBox: SHAPE): Region = {

    var rootAndAdjacentBoxes = rootBox :: rootBox.adjacentPartitions.filter {
      x => RegionCalculator.shouldAdjacentRegionBeIncludedInRegion(rootBox.regionId, x.regionId)
    }

    RegionCalculator.generateEmbracingRegion(rootAndAdjacentBoxes)
  }
  
}