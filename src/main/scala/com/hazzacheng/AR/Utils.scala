package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-5 
  * Time: 10:50 AM
  */
object Utils {

  def readAsRDD(sc: SparkContext,
                path: String): (RDD[Array[String]], RDD[Array[String]]) = {
    val dataRDD = sc.textFile(path + "D.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
  }

  def saveFreqItemset(
                       sc: SparkContext,
                       output: String,
                       freqItemsets: Array[(Set[Int], Int)],
                       freqItems: Array[String]
                     ): Unit = {
    val freqItemsBV = sc.broadcast(freqItems)
    sc.parallelize(freqItemsets).map { f =>
      val freqItemset = freqItemsBV.value
      f._1.toArray.sortBy(-_).map(freqItemset(_)).mkString(" ")
    }.sortBy(_).repartition(1).saveAsTextFile(output + "freqItems")

/*
    val strs = freqItemsets.map(f =>
      f._1.toArray.sortBy(-_).map(freqItems(_)).mkString(" ")).sorted
    sc.parallelize(strs).repartition(1).saveAsTextFile(output + "freqItems")
*/
  }

  def getAll(sc: SparkContext) = {
    val freqItemsetRDD = sc.textFile("/data/freqItemset")
    val freqItemsRDD = sc.textFile("/data/freqItems")
    val itemToRankRDD = sc.textFile("/data/itemToRank")


    val itemToRankTP = mutable.HashMap.empty[String, Int]
    itemToRankRDD.map(_.split(" ")).collect().foreach(x => itemToRankTP.put(x(0), x(1).toInt))
    val freqItemsTP = freqItemsetRDD.collect().sortBy(itemToRankTP(_))

    val freqItemsetTP = freqItemsetRDD.map{x =>
      val whole = x.split(" ")
      val count = whole.last.replace("[", "").replace("]", "")
      (whole.init.map(itemToRankTP(_)).toSet, count.toInt)
    }.collect()

    (freqItemsetTP, itemToRankTP, freqItemsTP)
  }


}
