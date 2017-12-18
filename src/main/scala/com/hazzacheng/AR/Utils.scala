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

  def formatOutput(freqItems: Array[FPGrowth.FreqItemset[String]],
                   total: Long): Array[String] = {
    val strs = freqItems.map{x =>
      val percent = x.freq.toDouble / total
      val str = x.items.mkString(",")

      str// + " ( " + x.freq + " " + percent + " )"
    }

    strs
  }

  def formattedSave(sc: SparkContext,
                    path: String,
                    freqItemsets: RDD[(Array[String], Int)],
                    itemToRank: mutable.HashMap[String, Int]): Unit = {
    val itemToRankBV = sc.broadcast(itemToRank)
    freqItemsets.map{x =>
      val itemToRank = itemToRankBV.value
      x._1.sortBy(itemToRank(_)).mkString(" ")
    }.saveAsTextFile(path)
  }

}
