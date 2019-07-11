package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialindex.rtree.{Entry, Point}

/**
 * Created by merlin on 9/20/15.
 */
object Util{

  def toEntry[K,V](k: K, value:V) :Entry[V]={
    Entry(k.asInstanceOf[Point],value)
  }

  def toPoint[K](k: K):  Point={
    k.asInstanceOf[Point]
  }

  //number of partitions for spatialRDD
  var numPartition=500

  //the percentage of sampling for the input data, this is used for build index
  def sampleRatio=0.01f
  //def get_spatial_rangx=1000
  //def get_spatial_rangy=1000

  //this one is used for local index
  var localIndex="RTREE"



}
