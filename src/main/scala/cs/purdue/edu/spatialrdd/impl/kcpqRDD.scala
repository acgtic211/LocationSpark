package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.spatialindex.rtree.{Box, Geom}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD

import scala.collection.immutable.{HashMap, IndexedSeq}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by merlin on 12/20/15.
 */
/**
 * because knn join is more complex than range join,
 * this class is specifically design for knn join function for spatial rdd
 */

/**
 * @param datardd spatialrdd
 * @param queryrdd points
 */
class kcpqRDD[K:ClassTag,V:ClassTag]
  (datardd:SpatialRDD[K,V],
   queryrdd:RDD[(K)],
   kcpq:Int,
   f1:(K)=>Boolean,
   f2:(V)=>Boolean
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
  def kcpquery():Array[(K,K,Double)]= {

    val kcpq = this.kcpq
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd = queryrdd.map(key => (key, kcpq))


    //val partitionedRDD =tmpqueryrdd.partitionBy(datardd.partitioner.get)
    val partitionedRDD = SpatialRDD.buildSRDDwithgivenPartitioner(tmpqueryrdd, datardd.partitioner.get).cache()

    val b1 = System.currentTimeMillis

    val stats = analysis(datardd, partitionedRDD)

    val topPartition = findSkewPartitions(stats)

    println(topPartition)

    println("Analysis time: " + (System.currentTimeMillis - b1) + " (ms)")

    val broadcastPartition = this.datardd.context.broadcast(topPartition)

    /*val localKnnJoinRDD2 = datardd.partitionsRDD.mapPartitionsWithIndex {
      (idx, iter) => {
        idx == broadcastPartition.value._1 match {
          case true => iter
          case false => Iterator.empty
        }
      }
    }*/

    /*val localKnnJoinRDD3 = partitionedRDD.partitionsRDD.mapPartitionsWithIndex(
      (idx, iter) => {
        idx == broadcastPartition.value._2 match {
          case true => iter
          case false => Iterator.empty
        }
      }
      , preservesPartitioning = true)*/

    //println(localKnnJoinRDD3.partitions.length)
    val localKnnJoinRDD3 = partitionedRDD.partitionsRDD.zipWithIndex()


    val betaRDD = datardd.partitionsRDD.zipPartitions(localKnnJoinRDD3, true) {
      (thisIter, otherIter) =>
        val otherPart = otherIter.next()
        if(otherPart._2 == broadcastPartition.value._2) {
          val thisPart = thisIter.next()
          thisPart.kcpquery_(otherPart._1, kcpq, None, None, f1, f2)
        }else{
          Iterator.empty
        }

    }

    val beta = betaRDD.top(kcpq)(new Ordering[(K, K, Double)] {
      override def compare(a: (K, K, Double), b: (K, K, Double)) = a._3.compare(b._3)
    })(kcpq - 1)._3

    val pq = betaRDD.collect()

    println("BETAAAAAA " + beta)

    val newStats = stats.filter(_._7 <= beta).map {
      case (id1, id2, size, size2, box, box2, dist, pddai) =>
        (id1, id2, box, box2)
    }

    //println(newStats.size)

    //newStats.foreach(println)


    val broadcastBeta = this.datardd.context.broadcast(beta)
    val broadcastPq = this.datardd.context.broadcast(pq)


    //val newStatsParallel = this.datardd.context.parallelize(newStats)

    val newStatsParallel = this.datardd.context.broadcast(newStats)


    /*val localKnnJoin2RDD = newStatsParallel.join(datardd.partitionsRDD.mapPartitionsWithIndex{
      (idx,iter)=> {
        Iterator((idx,iter.next()))
      }
    }).keyBy(_._2._1)


    val kkrdd = partitionedRDD.partitionsRDD.mapPartitionsWithIndex{
      (idx,iter)=> {
        Iterator((idx,iter.next()))
      }
    }.join(localKnnJoin2RDD)


    val localKnnJoinRDD = kkrdd.map {
      case (idx, (otherIter, (idx2, (idx3, thisIter)))) =>
        thisIter.kcpquery_(otherIter, kcpq, Some(broadcastBeta.value), Some(broadcastPq.value), f1, f2)

    }.flatMap(identity).top(kcpq)(new Ordering[(K, K, Double)] {
      override def compare(a: (K, K, Double), b: (K, K, Double)) = a._3.compare(b._3) * (-1)
    })

    */

    /*val joinTable = datardd.partitionsRDD.mapPartitionsWithIndex {
      (idx, iter) => {
        Iterator((idx, iter.next()))
      }
    }.flatMap{
      case (idx, iter) => {
        val result = newStatsParallel.value.filter(_._1==idx).map {
          case (id1, id2, box, box2) => {
            val beta = broadcastBeta.value.asInstanceOf[Float]
            val filteredIter = iter.filter(new Box(box2.x-beta,box2.y-beta,box2.x2+beta,box2.y2+beta),_=>true)
            (id2,SMapPartition(filteredIter))
          }
        }
        result
      }
    }


    val kkrdd = partitionedRDD.partitionsRDD.mapPartitionsWithIndex {
      (idx, iter) => {
        Iterator((idx, iter.next()))
      }
    }.flatMap{
      case (idx, iter) => {
        val result = newStatsParallel.value.filter(_._2==idx).map {
          case (id1, id2, box, box2) => {
            val beta = broadcastBeta.value.asInstanceOf[Float]
            val filteredIter = iter.filter(new Box(box.x-beta,box.y-beta,box.x2+beta,box.y2+beta),_=>true)
            (id2,SMapPartition(filteredIter))
          }
        }
        result
      }
    }*/

//    kkrdd.foreach(println)




    /*val joinTable = datardd.partitionsRDD.mapPartitionsWithIndex(
      (idx, iter) => {
        Iterator((idx, iter.next()))
      }, preservesPartitioning = true
    )*/


    val kkrdd = partitionedRDD.partitionsRDD.zipWithIndex().flatMap{
      case (iter, idx) => {
        val result = newStatsParallel.value.filter(_._2==idx).map {
          case (id1, id2, box1, box2) => {
            if (!(broadcastPartition.value._1 == id1 && broadcastPartition.value._2 == id2))
              (id1, iter)
            else {

              val map =new HashMap[K,V]
              (id1, new SMapPartition(map))
            }
          }
        }
        result
      }
    }.partitionBy(datardd.partitioner.get)

    println(kkrdd.partitions.length)



    /*val initialSet = mutable.HashSet.empty[(K, K, Double)]
    val addToSet = (h: mutable.HashSet[(K, K, Double)], items: Iterator[(K, K, Double)]) => {
      implicit val ord = Ordering.by[(K, K, Double), Double](_._3)
      val p1 = PriorityQueue.empty[(K, K, Double)]
      p1 ++= h
      items.foreach{
        item:(K,K,Double) => {
          /*if(!p1.exists(a => {
            a._1==item._1&&a._2==item._2
          }))*/
            p1 += item
          if (pq.size > kcpq) {
            p1.dequeue
          }
        }
      }
      h.clear()
      h ++= p1
    }
    val mergePartitionSets = (items1: mutable.HashSet[(K, K, Double)], items2: mutable.HashSet[(K, K, Double)]) => {
      implicit val ord = Ordering.by[(K, K, Double), Double](_._3)
      val p1 = PriorityQueue.empty[(K, K, Double)]
      p1 ++= items1
      items2.toIterator.foreach{
        item:(K,K,Double) => {
          /*if(!p1.exists(a => {
            a._1==item._1&&a._2==item._2
          }))*/
            p1 += item
          if (p1.size > kcpq) {
            p1.dequeue
          }
        }
      }
      items1.clear()
      items1 ++= p1
    }
*/

    println(datardd.partitioner);
    println(kkrdd.partitioner);
   val localKnnJoinRDD = datardd.partitionsRDD.zipPartitions(
     kkrdd, preservesPartitioning = true){
      case (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        implicit val ord = Ordering.by[(K, K, Double), Double](_._3)
        val p1 = mutable.PriorityQueue.empty[(K, K, Double)]
        p1 ++= broadcastPq.value
        var beta = broadcastBeta.value
        otherIter.foreach(
          otherPart => {
            val result = thisPart.kcpquery_(otherPart._2, kcpq, Some(beta), Some(p1.toIterator), f1, f2)

            p1.clear()
            p1++=result
            beta = pq.head._3

          }
        )
        p1.toIterator

   }/*.flatMap(identity).aggregateByKey(initialSet)(addToSet,mergePartitionSets).map{
      case (key, iter) => {
        iter
      }
    }.reduce(mergePartitionSets)*/.distinct().top(kcpq)(new Ordering[(K, K, Double)] {
      override def compare(a: (K, K, Double), b: (K, K, Double)) = a._3.compare(b._3) * (-1)
    })

    localKnnJoinRDD.toArray.sortBy(_._3)

    //ArrayBuffer.empty[(K, K, Double)].toArray
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

