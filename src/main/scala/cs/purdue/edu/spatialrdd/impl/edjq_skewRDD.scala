package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.scheduler.skewAnalysis
import cs.purdue.edu.spatialindex.quatree.QtreeForPartion
import cs.purdue.edu.spatialindex.rtree.{Box, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.IndexedSeq
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
class edjq_skewRDD[K:ClassTag,V:ClassTag]
  (datardd:SpatialRDD[K,V],
   queryrdd:RDD[(K)],
   epsilon:Double,
   f1:(K)=>Boolean,
   f2:(V)=>Boolean
    ) extends Serializable
{


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

  private def getPartitionSize[T](rdd: SpatialRDD[K,T]): Array[(Int, Int)] = {

    val sketched = rdd.partitionsRDD.mapPartitionsWithIndex{ (idx, iter) =>
      val thisPart = iter.next()
      Iterator((idx,thisPart.size.toInt))
    }.collect()
    sketched
  }


  /**
   *this is the rangebased knn join
    *
    * @return
   */
  def kcpquery():RDD[(K,K,Double)]= {

    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd = SpatialRDD.buildSRDDwithgivenPartitioner(queryrdd.map(key => (key, epsilon)),this.datardd.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val broadcastEpsilon = this.datardd.context.broadcast(epsilon)


    val stat=analysis(this.datardd,tmpqueryrdd)

    val topKpartitions=skewAnalysis.findSkewPartitionQuery(stat,0.5)

    val broadcastVar = this.datardd.context.broadcast(topKpartitions)

    //transform the skew and query rdd
    val skew_queryrdd = tmpqueryrdd.mapPartitionsWithIndex{
      (pid,iter)=>
        broadcastVar.value.contains(pid) match
        {
          case true=> iter
          case false=> Iterator.empty
        }
    }

    val skew_datardd= this.datardd.mapPartitionsWithIndex(
      (pid,iter)=> broadcastVar.value.contains(pid) match
      {
        case true=>iter
        case false=>Iterator.empty
      },true
    )

    val nonskew_queryrdd = tmpqueryrdd.mapPartitionsWithIndex{
      (pid,iter)=>
        broadcastVar.value.contains(pid) match
        {
          case false=> iter
          case true=> Iterator.empty
        }
    }

    val nonskew_datardd= this.datardd

    /**
      * below the the option 2, get the new data partitioner based on the query, then do the join
      */
    val newpartitioner=getPartitionerbasedQuery(topKpartitions,skew_queryrdd)
    val skewindexrdd=SpatialRDD.buildSRDDwithgivenPartitioner(skew_datardd,newpartitioner)

    //val part1=skewindexrdd.sjoins[U](skew_queryrdd)((k, id) => id)
    val partitioned = skew_queryrdd.partitionBy(skewindexrdd.partitioner.get)
    val part1=skewindexrdd.partitionsRDD.zipPartitions(
      partitioned, preservesPartitioning = true)
    {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        thisPart.edjquery_(otherIter, broadcastEpsilon.value, f1, f2)
    }

    part1
//    /*************************************************************/
//    val partitioned2 = nonskew_queryrdd.partitionBy(nonskew_datardd.partitioner.get)
//    val part2=nonskew_datardd.partitionsRDD.zipPartitions(
//      partitioned2, preservesPartitioning = true)
//    {
//      (thisIter, otherIter) =>
//        val thisPart = thisIter.next()
//        thisPart.edjquery_(otherIter, broadcastEpsilon.value, f1, f2)
//    }
//    /*println("SKEEEEW")
//    println(part1.count())
//    println("NON SKEEEEW")
//    println(part2.count())*/
//
//
//
//
//
//
//      val nonskew_datarddBox = nonskew_datardd.partitionsRDD.mapPartitionsWithIndex(
//        (idx, iter) => {
//          val otherPart = iter.next()
//          val box = otherPart.box
//          val betaF = broadcastEpsilon.value.asInstanceOf[Float]
//          Iterator((idx, Box(box.x - betaF, box.y - betaF, box.x2 + betaF, box.y2 + betaF)))
//        }
//        , true)
//
//      val nonskew_datarddPointsWithBox = nonskew_datarddBox.map {
//        case (idx, box) =>
//          this.datardd.partitioner.get match {
//            case qtreepartition: QtreePartitioner[K, V] =>
//              (idx, qtreepartition.getPointsForSJoin(box), box)
//          }
//      }
//
//      val part3 = nonskew_datardd.partitionsRDD.zipPartitions(
//        nonskew_datarddPointsWithBox, preservesPartitioning = true) {
//        (thisIter, otherIter) =>
//          val thisPart = thisIter.next()
//          val otherPart = otherIter.next()
//          thisPart.edjquery_(otherPart._2.map(p => (p.asInstanceOf[K], 1)).toIterator, broadcastEpsilon.value, f1, f2)
//      }
//
//      //nonskew_datarddPointsWithBox.collect().foreach(println)
//
//      val skew_datarddBox = skewindexrdd.partitionsRDD.mapPartitionsWithIndex(
//        (idx, iter) => {
//          val otherPart = iter.next()
//          val box = otherPart.box
//          val betaF = broadcastEpsilon.value.asInstanceOf[Float]
//          Iterator((idx, Box(box.x - betaF, box.y - betaF, box.x2 + betaF, box.y2 + betaF)))
//        }
//
//        , true)
//
//      val skew_datarddPointsWithBox = skew_datarddBox.map {
//        case (idx, box) =>
//          this.datardd.partitioner.get match {
//            case qtreepartition: QtreePartitioner[K, V] =>
//              (idx, qtreepartition.getPointsForSJoin(box), box)
//          }
//      }
//
//      val part4 = skewindexrdd.partitionsRDD.zipPartitions(
//        skew_datarddPointsWithBox, preservesPartitioning = true) {
//        (thisIter, otherIter) =>
//          val thisPart = thisIter.next()
//          val otherPart = otherIter.next()
//          thisPart.edjquery_(otherPart._2.map(p => (p.asInstanceOf[K], 1)).toIterator, broadcastEpsilon.value, f1, f2)
//      }
//
//      /*val union1 = part1 union part3
//      val union2 = part2 union part4*/
//
//
//      val result = this.datardd.context.union(part1,part2,part3,part4)

    //println("PREBETAAAAAA " + preBeta)

    //ArrayBuffer.empty[(K, K, Double)].toArray
    //result
  }


  /**
   * todo: add the herusitic way to search for the range knn join
   */
  def herusticknnjoin()={

  }

  /**
   * get the statistic information for the input rdd
   */
  private def analysis[T](data:SpatialRDD[K,V], query:RDD[(K,T)]):IndexedSeq[(Int,Int,Int)]=
  {

    val stat_datardd=getPartitionSize(data).sortBy(_._1)
    val stat_queryrdd=getPartitionSize(query).sortBy(_._1)

    val result = stat_datardd.map{
      case(id,size)=>
        stat_queryrdd.find(_._1==id).getOrElse(None)
        match{
          case (qid:Int,qsize:Int)=> (id,size,qsize)
          case None=>(id,size,0)
        }
    }


    result.filter(p => (p._2!=0)&&(p._3!=0)).toIndexedSeq
  }

  /**
    * get the partitioner based on the query distribution
    * @param topKpartitions
    */
  private def getPartitionerbasedQuery[T](topKpartitions:Map[Int,Int], skewQuery:RDD[(K,T)]): QtreePartitionerBasedQueries[Int,QtreeForPartion] =
  {
    //default nubmer of queries
    val samplequeries=skewQuery.sample(false,0.02f).map{case(point, kk)=>point}.collect()

    //get the quadtree partionner from this data rdd, and colone that quadtree
    val qtreepartition=new QtreeForPartion(100)
    this.datardd.partitioner.getOrElse(None) match {
      case qtreepter: QtreePartitioner[K, V] =>
        val newrootnode=qtreepter.quadtree.coloneTree()
        //qtreepter.quadtree.printTreeStructure()
        qtreepartition.root=newrootnode
    }

    //run those queries over the old data partitioner
    samplequeries.foreach
    {
      case point:Point=>qtreepartition.visitleafForBox(point.toBox);
    }


    //get the new partitionid based on those queries
    val partitionnumberfromQueries= qtreepartition.computePIDBasedQueries(topKpartitions)

    new QtreePartitionerBasedQueries(partitionnumberfromQueries,qtreepartition)

  }


}

