package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth


/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-4 
  * Time: 8:35 PM
  */
object TestMain {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val input = args(0)
    val output = args(1)
    val minSupport = 0.092
    val len = args(2).toInt
    val (dataRDD, userRDD) = RddUtils.readAsRDD(sc, input)

    val newSize = dataRDD.count()
//    val newRDD = RddUtils.removeRedundancy(sc, dataRDD, (size * minSupport).toInt)
//    val newSize = newRDD.count()
    //newRDD.map(_.mkString(" ")).saveAsTextFile("/RDD")

//    val dataRdd = dataRDD.filter(_.length < len).zipWithIndex().filter(_._2 < 100000).map(_._1).persist(StorageLevel.MEMORY_AND_DISK_SER)
//    val size = dataRdd.count()
//    val percent = size.toDouble / total.toDouble
//    println("==== Size < " + len + " :" + size + " " + percent)


    val freqItems = new FPGrowth().setMinSupport(0.092).run(dataRDD).freqItemsets.collect()
    val strs = RddUtils.formatOutput(freqItems, newSize)
    val lenStr = strs.length
    println("==== FreqItems Size < " + len + " :" + lenStr)

    sc.parallelize(strs).saveAsTextFile(output)

  }
}
