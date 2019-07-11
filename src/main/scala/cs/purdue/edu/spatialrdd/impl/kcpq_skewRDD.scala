package cs.purdue.edu.spatialrdd.impl

import cs.purdue.edu.scheduler.skewAnalysis
import cs.purdue.edu.spatialindex.quatree.QtreeForPartion
import cs.purdue.edu.spatialindex.rtree.{Box, Geom, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.reflect.ClassTag
import scala.util.control.Breaks._

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
class kcpq_skewRDD[K:ClassTag,V:ClassTag]
  (datardd:SpatialRDD[K,V],
   queryrdd:RDD[(K)],
   kcpq:Int,
   filter:Boolean,
   f1:(K)=>Boolean,
   f2:(V)=>Boolean
    ) extends Serializable
{


  private def getPartitionStats[T](rdd: SpatialRDD[K,T]): (Array[(Int, Int, Box)]) = {

    val sketched = rdd.partitionsRDD.zipWithIndex.map{ (tupla) =>
        (tupla._2.toInt,tupla._1.size.toInt, tupla._1.box)
    }.collect()
    sketched
  }

  private def getPartitionSize[T](rdd: SpatialRDD[K,T]): Array[(Int, Int)] = {

    val sketched = rdd.partitionsRDD.zipWithIndex.map{ (tupla) =>
      (tupla._2.toInt,tupla._1.size.toInt)
    }.collect()
    sketched
  }

  var stat_datardd:Array[(Int, Int, Box)] = null;

  var stat_queryrdd:Array[(Int, Int)] = null;

  /**
   *this is the rangebased knn join
    *
    * @return
   */
  def kcpquery():Array[(K,K,Double)]= {

    val kcpq = this.kcpq
    //step1: partition the queryrdd if the partitioner of query and data rdd is different
    val tmpqueryrdd = SpatialRDD.buildSRDDwithgivenPartitioner(queryrdd.map(key => (key, kcpq)),this.datardd.partitioner.get).persist(StorageLevel.MEMORY_AND_DISK_SER)

    this.stat_queryrdd = getPartitionSize(tmpqueryrdd);
    this.stat_datardd = getPartitionStats(this.datardd);

    val stats = analysis();

    val prefiltering = this.filter match {
      case true => {
        Some(analysisBeta(this.datardd,tmpqueryrdd,stats).sortWith(_._3<_._3))
      }
      case false =>{
        None
      }
    }

    //prefiltering.foreach(println)

    val preBeta = prefiltering match {
      case Some(value) => {
        Some(value(kcpq - 1)._3)
      }
      case None => None
    }

    println("BETAAAAAA " + preBeta)

    val broadcastPreBeta = this.datardd.context.broadcast(preBeta)
    val broadcastPrePq = this.datardd.context.broadcast(prefiltering)

    val theStats = stats.map((tupla)=>(tupla._1,tupla._2,tupla._3)).toIndexedSeq

    val topKpartitions=skewAnalysis.findSkewPartitionQuery(theStats,0.5)

    topKpartitions.foreach(println)

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
        val pq = broadcastPrePq.value match {
          case Some(value) => Some(value.toIterator)
          case None => None
        }
        thisPart.kcpquery_(otherIter, kcpq, broadcastPreBeta.value, pq, f1, f2)
    }

    /*************************************************************/
    val partitioned2 = nonskew_queryrdd.partitionBy(nonskew_datardd.partitioner.get)
    val part2=nonskew_datardd.partitionsRDD.zipPartitions(
      partitioned2, preservesPartitioning = true)
    {
      (thisIter, otherIter) =>
        val thisPart = thisIter.next()
        val pq = broadcastPrePq.value match {
          case Some(value) => Some(value.toIterator)
          case None => None
        }
        thisPart.kcpquery_(otherIter, kcpq, broadcastPreBeta.value, pq, f1, f2)
    }
    /*println("SKEEEEW")
    println(part1.count())
    println("NON SKEEEEW")
    println(part2.count())*/

    val partUnion = part1 union part2

    implicit val ord = Ordering.by[(K, K, Double), Double](_._3).reverse

    val pq = partUnion.distinct().top(kcpq)

    println("SAME PARTITION")
    pq.foreach(println)

    val beta = pq(kcpq - 1)._3

    println("BETAAAAAA " + beta)

    var result = pq

    if(beta > 0) {
      val broadcastBeta = this.datardd.context.broadcast(beta)
      val broadcastPq = this.datardd.context.broadcast(pq)

      val nonskew_datarddBox = nonskew_datardd.partitionsRDD.mapPartitionsWithIndex(
        (idx, iter) => {
          val otherPart = iter.next()
          val box = otherPart.box
          val betaF = broadcastBeta.value.asInstanceOf[Float]
          Iterator((idx, Box(box.x - betaF, box.y - betaF, box.x2 + betaF, box.y2 + betaF)))
        }

        , true)

      val nonskew_datarddPointsWithBox = nonskew_datarddBox.map {
        case (idx, box) =>
          this.datardd.partitioner.get match {
            case qtreepartition: QtreePartitioner[K, V] =>
              (idx, qtreepartition.getPointsForSJoin(box), box)
          }
      }

      val part3 = nonskew_datardd.partitionsRDD.zipPartitions(
        nonskew_datarddPointsWithBox, preservesPartitioning = true) {
        (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          val otherPart = otherIter.next()
          thisPart.kcpquery_(otherPart._2.map(p => (p.asInstanceOf[K], 1)).toIterator, kcpq, Some(broadcastBeta.value), Some(broadcastPq.value.toIterator), f1, f2)
      }

      //nonskew_datarddPointsWithBox.collect().foreach(println)

      val skew_datarddBox = skewindexrdd.partitionsRDD.mapPartitionsWithIndex(
        (idx, iter) => {
          val otherPart = iter.next()
          val box = otherPart.box
          val betaF = broadcastBeta.value.asInstanceOf[Float]
          Iterator((idx, Box(box.x - betaF, box.y - betaF, box.x2 + betaF, box.y2 + betaF)))
        }

        , true)

      val skew_datarddPointsWithBox = skew_datarddBox.map {
        case (idx, box) =>
          this.datardd.partitioner.get match {
            case qtreepartition: QtreePartitioner[K, V] =>
              (idx, qtreepartition.getPointsForSJoin(box), box)
          }
      }

      val part4 = skewindexrdd.partitionsRDD.zipPartitions(
        skew_datarddPointsWithBox, preservesPartitioning = true) {
        (thisIter, otherIter) =>
          val thisPart = thisIter.next()
          val otherPart = otherIter.next()
          thisPart.kcpquery_(otherPart._2.map(p => (p.asInstanceOf[K], 1)).toIterator, kcpq, Some(broadcastBeta.value), Some(broadcastPq.value.toIterator), f1, f2)
      }

      val unionResult = part3 union part4

      result = unionResult.distinct.top(kcpq)
    }

    println("PREBETAAAAAA " + preBeta)

    ArrayBuffer.empty[(K, K, Double)].toArray
    result
  }


  /**
   * todo: add the herusitic way to search for the range knn join
   */
  def herusticknnjoin()={

  }

  /**
   * get the statistic information for the input rdd
   */
  private def analysis[T]():Array[(Int,Int,Int,Box,Float)]=
  {

    val result = stat_datardd.map{
      case(id,size,box)=> {
        val defaultPdai = 0.0
        stat_queryrdd.find(_._1==id).getOrElse(None)
        match{
          case (id2:Int,size2:Int)=> {
            if(size==0||size2==0)
              (id,size,0,box,defaultPdai.toFloat)
            else {
              val unionArea = box.area
              val pddai = (size + size2) / unionArea + (size + size2)
              (id, size, size2, box, pddai)
            }
          }
          case _=>(id,size,0,box,defaultPdai.toFloat)
        }
      }
    }


    result.filter(p => (p._2!=0)&&(p._3!=0))
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


  private def analysisBeta[T:ClassTag](data:SpatialRDD[K,V], query:SpatialRDD[K,T], stats:Array[(Int,Int,Int,Box,Float)]):Array[(K, K, Double)]=
  {

    val topPartition = stats.sortWith(_._5>_._5).slice(0,1).map(elem=>(elem._1)).reduce((a,b)=>a)

    println(topPartition)

    val broadcastPIDToQuery = data.context.broadcast(topPartition)


    val dataFiltered = data.mapPartitionsWithIndex((index,iter) => {
      broadcastPIDToQuery.value == index match
      {
        case true=>{
          //println("esta si es "+index)
          iter
        }
        case false=>Iterator.empty
      }
    },true)

    val queryFiltered = query.mapPartitionsWithIndex((index,iter) => {
        broadcastPIDToQuery.value == index match
        {
          case true=>{
            //println("esta si es "+index)
            iter
          }
          case false=>Iterator.empty
        }
    },true)

    /*var dataElementCount = data.count() * 0.001
    dataElementCount  = if(dataElementCount > kcpq) dataElementCount else kcpq
    dataElementCount  = if(dataElementCount > 1000000) 1000000 else dataElementCount


    val dataSampled = dataFiltered.takeSample(false,dataElementCount.toInt,1)

    var queryElementCount = query.count() * 0.001
    queryElementCount  = if(queryElementCount > kcpq) queryElementCount else kcpq
    queryElementCount  = if(queryElementCount > 1000000) 1000000 else dataElementCount


    val querySampled = queryFiltered.takeSample(false,queryElementCount.toInt,1)*/

    val dataSampled = dataFiltered.takeSample(false,100000)
    val querySampled = queryFiltered.takeSample(false,100000)

    implicit val ord = Ordering.by[(K, K, Double), Double](_._3).reverse


    val pq = kcpquery_(dataSampled,querySampled,kcpq)

    pq.toArray


    //ArrayBuffer.empty[(K,K,Double)].toArray


  }

  def kcpquery_[T: ClassTag]
  (sortedData: Array[(K, V)], otherData: Array[(K, T)], kcpq:Int):  PriorityQueue[(K, K, Double)]=
  {

    println("SORTED DATA "+sortedData.length)
    println("otherData DATA "+otherData.length)
    implicit val ord2 = Ordering.by[(K,V),Float](_._1.asInstanceOf[Geom].toBox.x)

    implicit val ord3 = Ordering.by[(K,T),Float](_._1.asInstanceOf[Geom].toBox.x)

    scala.util.Sorting.quickSort(sortedData)

    scala.util.Sorting.quickSort(otherData)


    var i = 0
    var j = 0
    val gTotPoints1 = sortedData.length
    val gTotPoints2 = otherData.length

    implicit val ord = Ordering.by[(K,K,Double),Double](_._3)

    val pq = PriorityQueue.empty[(K,K,Double)]


    var gdmax = Double.PositiveInfinity

    //println(gTotPoints1+":"+gTotPoints2+":"+gdmax)


    while(i<gTotPoints1&&j<gTotPoints2){
      val p = sortedData(i)._1.asInstanceOf[Point]
      val q = otherData(j)._1.asInstanceOf[Point]

      if(p.x<q.x){
        val refo = p

        breakable {
          for (k <- j until gTotPoints2) {
            val curo = otherData(k)._1.asInstanceOf[Point]
            val dx = curo.x - refo.x
            if (dx >= gdmax) {
              if(dx > gdmax || pq.size == kcpq)
                break
            }
            val d = refo.distance(curo)
            if (d < gdmax || (d == gdmax && pq.size < kcpq)) {
              pq += ((refo.asInstanceOf[K], curo.asInstanceOf[K], d))
              if (pq.size > kcpq) {
                pq.dequeue
                gdmax = pq.head._3
                //println(gdmax)
              }
            }
          }
        }

        i=i+1
      }else{
        val refo = q

        breakable {
          for (k <- i until gTotPoints1) {
            val curo = sortedData(k)._1.asInstanceOf[Point]
            val dx = curo.x - refo.x
            if (dx >= gdmax) {
              if(dx > gdmax || pq.size == kcpq)
                break
            }
            val d = refo.distance(curo)
            if (d < gdmax || (d == gdmax && pq.size < kcpq)) {
              pq += ((curo.asInstanceOf[K], refo.asInstanceOf[K], d))
              if (pq.size > kcpq) {
                pq.dequeue
                gdmax = pq.head._3
                //println(gdmax)

              }
            }
          }
        }

        j=j+1
      }
    }
    pq

  }

}

