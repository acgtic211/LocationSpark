package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialindex.rtree.{Box, Geom, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random._

/**
 * Created by merlin on 12/20/15.
 */
/**
 * because knn join is more complex than range join,
 * this class is specifically design for knn join function for spatial rdd
 */

/**
 * @param datardd spatialrdd
 * @param queryPoint point
 */
class randomRDD[K:ClassTag,V:ClassTag]
  (datardd:SpatialRDD[K,V],
   queryPoint:Point,
   k:Int
   ) extends Serializable
{

  private def getPartitionStats[T](rdd: RDD[(K,T)]): (Array[(Int, Int, Box)]) = {
    def toBox(thing: (K,T)): Box = thing._1.asInstanceOf[Geom].toBox
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      if(iter.nonEmpty){
        val tempBox = iter.map(thing => {
          (toBox(thing),1)
        }).reduce((a1,b1)=>{
          val a = a1._1
          val b = b1._1
          (Box(Math.min(a.x,b.x),Math.min(a.y,b.y),Math.max(a.x2,b.x2),Math.max(a.y2,b.y2)),a1._2+b1._2)
        })
        Iterator((idx, tempBox._2, tempBox._1))
      }else{
        Iterator((idx,iter.size, Box.empty))
      }
    }.collect()
    sketched
  }

  private def getPartitionStats[T](rdd: SpatialRDD[K,T]): (Array[(Int, Long, Box)]) = {

    val sketched = rdd.partitionsRDD.mapPartitionsWithIndex{ (idx, iter) =>
        val thisPart = iter.next()
        Iterator((idx,thisPart.size, thisPart.box))
    }.collect()
    sketched
  }

  private def getPartitionSize[T](rdd: RDD[(K,T)]): Array[(Int, Int)] = {

    val sketched = rdd.mapPartitionsWithIndex{ (idx, iter) =>
      Iterator((idx,iter.size))
    }.collect()
    sketched
  }



  /**
   *this is the rangebased knn join
    *
    * @return
   */
  def rnnquery():Array[(K,Double)]= {
    def getRandomUniformFloatPoint(startx:Float, starty:Float, rangx:Float, rangy:Float):Point=
      Point((nextFloat()*rangx)+startx, (nextFloat()*rangy)+starty)

    val firstRound = this.datardd.partitionsRDD.mapPartitions(
      iter => {
        val thisPart = iter.next()
        val thisBox = thisPart.box
        //Iterator((thisBox.x.toInt,thisBox.y.toInt,(thisBox.x2-thisBox.x).toInt,(thisBox.y2-thisBox.y).toInt,thisPart.size))
        if((thisBox.x2-thisBox.x)>0&&(thisBox.y2-thisBox.y)>0) {
          val r = getRandomUniformFloatPoint(thisBox.x, thisBox.y, (thisBox.x2 - thisBox.x), (thisBox.y2 - thisBox.y))
          Iterator((r, thisPart.size))
        }
        else
          Iterator.empty

      }

    )

    firstRound.collect().sortWith(_._2>_._2).take(10).foreach(println)

    ArrayBuffer.empty[(K, Double)].toArray
  }


  /**
   * todo: add the herusitic way to search for the range knn join
   */
  def herusticknnjoin()={

  }

  /**
   * get the statistic information for the input rdd
   */
  private def analysis[T](data:SpatialRDD[K,V], query:RDD[(K,T)]):IndexedSeq[(Int, Int, Long, Long, Double, Double)]=
  {

    val stat_datardd=getPartitionStats(data)

    val stat_queryrdd=getPartitionSize(query)

    val stat_queryrdd_box = stat_datardd.map{
      case(id,size,box)=>
        stat_queryrdd.find(_._1==id).getOrElse(None)
        match{
          case (qid:Int,qsize:Int)=> (id,qsize,box)
          case None=>(id,0,box)
        }
    }.toList

    stat_datardd.flatMap{
      case(id,size,box)=> {
        val buffer = ArrayBuffer.empty[(Int,Int,Long,Long,Double,Double)]
        stat_queryrdd_box.foreach{
          case(id2,size2,box2)=>
            if(size==0||size2==0)
              buffer.append((id,id2,size,size2,Double.PositiveInfinity,0))
            else {
              val unionArea = box2.expand(box).area
              val interArea = box2.intersectionarea(box)
              val pddai = (size + size2) / unionArea * (1 + interArea)
              val dist = box2.mindistance(box)
              buffer.append((id, id2, size, size2, dist, pddai))
            }

        }
        buffer.toIterator
      }
    }.toIndexedSeq

  }

  private def analysis[T](data:SpatialRDD[K,V], query:SpatialRDD[K,T]):IndexedSeq[(Int, Int, Long, Long, Box, Box, Double, Double)]=
  {

    val stat_datardd=getPartitionStats(data)

    val stat_queryrdd=getPartitionStats(query)

    stat_datardd.flatMap{
      case(id,size,box)=> {
        val buffer = ArrayBuffer.empty[(Int,Int,Long,Long,Box,Box,Double,Double)]
        stat_queryrdd.foreach{
          case(id2,size2,box2)=>
            if(size==0||size2==0)
              buffer.append((id,id2,size,size2,box, box2, Double.PositiveInfinity,0))
            else {
              val unionArea = box2.expand(box).area
              val interArea = box2.intersectionarea(box)
              val pddai = (size + size2) / unionArea * (1 + interArea)
              val dist = box2.mindistance(box)
              buffer.append((id, id2, size, size2, box, box2, dist, pddai))
            }

        }
        buffer.toIterator
      }
    }.toIndexedSeq

  }

  def findSkewPartitions(stats: IndexedSeq[(Int, Int, Long, Long, Box, Box, Double, Double)]):(Int,Int) = {
    stats.sortWith(_._8>_._8).slice(0,1).map(elem=>(elem._1,elem._2)).reduce((a,b)=>a)
  }

}

