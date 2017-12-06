package com.hazzacheng.AR.utils

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
object RddUtils {

  def readAsRDD(sc: SparkContext,
                path: String): (RDD[Array[String]], RDD[Array[String]]) = {
    val dataRDD = sc.textFile(path + "build_data", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
  }

  def formatOutput(freqItems: Array[FPGrowth.FreqItemset[String]],
                   total: Long): Array[String] = {
    val strs = freqItems.map{x =>
      val percent = x.freq.toDouble / total
      val str = x.items.mkString("[", ",", "]")

      str + " ( " + x.freq + " " + percent + " )"
    }

    strs
  }

  def removeRedundancy(sc: SparkContext,
                       data: RDD[Array[String]],
                       min: Int): RDD[Array[String]] = {
    val itemsSet = mutable.HashSet.empty[String]
    data.flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .filter(_._2 >= min).collect()
      .foreach(x => itemsSet.add(x._1))

    val size = itemsSet.size
    println("==== items count: " + size)

    val itemsBV = sc.broadcast(itemsSet)
    val newRdd = data.map(x => x.filter(itemsBV.value.contains(_)))
      .filter(_.nonEmpty)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val rddSize = newRdd.count()
    println("==== transaction count: " + rddSize)

    newRdd
  }

}
