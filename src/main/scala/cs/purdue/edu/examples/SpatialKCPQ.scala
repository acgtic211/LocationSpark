package cs.purdue.edu.examples

import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.{Util, kcpq_skewRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by merlin on 6/8/16.
 */
object SpatialKCPQ {

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
        case "--k" :: value :: tail =>
          nextOption(map ++ Map('k -> value), tail)
        case "--index" :: value :: tail =>
          nextOption(map = map ++ Map('index -> value), list = tail)
        case "--numPartitions" :: value :: tail =>
          nextOption(map = map ++ Map('numPartitions -> value), list = tail)
        case "--filter" :: value :: tail =>
          nextOption(map = map ++ Map('filter -> value), list = tail)
        case option :: tail => println("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
    val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
    Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]

    val k=options.getOrElse('k, Nil).toString.toInt
    Util.numPartition = options.getOrElse('numPartitions, "500").toString.toInt
    val filter = options.getOrElse('filter, "false").toString.toBoolean




    val conf = new SparkConf().setAppName("Test for Spatial JOIN SpatialRDD")//.setMaster("local[2]")

    val spark = new SparkContext(conf)

    /** **********************************************************************************/
    //this is for WKT format for the left data points
    val leftpoints = spark.textFile(leftFile).map {
      case x => {
        val corrds = x.split(",")
        var cordX = corrds(0).toFloat
        var cordY = corrds(1).toFloat
        cordX = if (cordX > 180) 180 else cordX
        cordX = if (cordX < (-180)) -180 else cordX
        cordY = if (cordY > (90)) 90 else cordY
        cordY = if (cordY < (-90)) -90 else cordY

        (Point(cordX, cordY), "1")
      }
    }


    val leftLocationRDD = SpatialRDD(leftpoints).persist(StorageLevel.MEMORY_AND_DISK_SER);
    /** **********************************************************************************/

    /** **********************************************************************************/
    val rightData = spark.textFile(rightFile)
    val rightpoints = rightData.map {
      case x => {
        val corrds = x.split(",")
        var cordX = corrds(0).toFloat
        var cordY = corrds(1).toFloat
        cordX = if (cordX > 180) 180 else cordX
        cordX = if (cordX < (-180)) -180 else cordX
        cordY = if (cordY > (90)) 90 else cordY
        cordY = if (cordY < (-90)) -90 else cordY

        Point(cordX, cordY)
      }
    }


    println("global index: "+ Util.localIndex+" ; local index: "+ Util.localIndex)

    /** **********************************************************************************/
    val b1 = System.currentTimeMillis

    val kcpq=new kcpq_skewRDD[Point,String](leftLocationRDD,rightpoints,k,filter,(id)=>true,(id)=>true)

    val kcpqresult=kcpq.kcpquery()

    kcpqresult.foreach(println)

    println("spatial range join time: "+(System.currentTimeMillis - b1) +" (ms)")

    spark.stop()

  }

}
