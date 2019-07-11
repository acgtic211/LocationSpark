package cs.purdue.edu.examples

import cs.purdue.edu.spatialindex.rtree.Box
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by merlin on 6/8/16.
 */
object SpatialEDJQMBR {

  val usage = """
    Implementation of Spatial Join on Spark
    Usage: spatialjoin --left left_data
                       --right right_data
                       --index the local index for spatial data (default:rtree)
                       --k the K-nearest-neighbor
                       --help
              """

  def main(args: Array[String]) {

    if(args.length==0) println(usage)

    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--help" :: tail =>
          println(usage)
          sys.exit(0)
        case "--left" :: value :: tail =>
          nextOption(map ++ Map('left -> value), tail)
        case "--right" :: value :: tail =>
          nextOption(map ++ Map('right -> value), tail)
        case "--epsilon" :: value :: tail =>
          nextOption(map ++ Map('epsilon -> value), tail)
        case "--index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value), list = tail)
        case "--numPartitions" :: value :: tail =>
          nextOption(map = map ++ Map('numPartitions -> value), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
    val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
    Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]

    val epsilon=options.getOrElse('epsilon, Nil).toString.toDouble
    val numPartitions = options.getOrElse('numPartitions, "500").toString.toInt



    val conf = new SparkConf().setAppName("Test for Spatial JOIN SpatialRDD")//.setMaster("local[2]")

    val spark = new SparkContext(conf)

    /** **********************************************************************************/
    //this is for WKT format for the left data points
    val leftpoints = spark.textFile(leftFile).map {
      case x => {
        val corrds = x.split(",")
        var cordX = corrds(0).toFloat
        var cordY = corrds(1).toFloat
        var cordX2 = corrds(2).toFloat
        var cordY2 = corrds(3).toFloat
        cordX = if (cordX > 180) 180 else cordX
        cordX = if (cordX < (-180)) -180 else cordX
        cordY = if (cordY > (90)) 90 else cordY
        cordY = if (cordY < (-90)) -90 else cordY
        cordX2 = if (cordX2 > 180) 180 else cordX2
        cordX2 = if (cordX2 < (-180)) -180 else cordX2
        cordY2 = if (cordY2 > (90)) 90 else cordY2
        cordY2 = if (cordY2 < (-90)) -90 else cordY2

        (Box(cordX, cordY, cordX2, cordY2), "1")
      }
    }


    val leftLocationRDD = SpatialRDD.buildSPRDDwithPartitionNumber(leftpoints,numPartitions).persist(StorageLevel.MEMORY_AND_DISK_SER)
    /** **********************************************************************************/

    /** **********************************************************************************/
    val rightData = spark.textFile(rightFile)
    val rightpoints = rightData.map {
      case x => {

        val coords = x.split(",")
        val cordX = Math.max(Math.min(180,coords(0).toFloat),180)
        val cordY = Math.max(Math.min(90,coords(1).toFloat),-90)
        val cordX2 = Math.max(Math.min(180,coords(2).toFloat),180)
        val cordY2 = Math.max(Math.min(90,coords(3).toFloat),-90)

        Box(cordX, cordY, cordX2, cordY2)
      }
    }


    println("global index: "+ Util.localIndex+" ; local index: "+ Util.localIndex)

    /** **********************************************************************************/
    val b1 = System.currentTimeMillis

    //val kcpq=new edjq_skewRDD[Box,String](leftLocationRDD,rightpoints,epsilon, (id)=>true, (id)=>true)

///    val kcpqresult=kcpq.kcpquery()

   // val tuples=kcpqresult.map{case(a,b,v)=>(1,1)}.reduceByKey{case(a,b)=>{a+b}}.map{case(a,b)=>b}.collect()

    println("global index: "+ Util.localIndex+" ; local index: "+ Util.localIndex)
    //println("query results size: "+tuples(0))
    //val tuples=kcpqresult.map{case(a,b,v)=>(1,v.size)}.reduceByKey{case(a,b)=>{a+b}}.map{case(a,b)=>b}.collect()

    println("spatial range join time: "+(System.currentTimeMillis - b1) +" (ms)")

    spark.stop()

  }

}
