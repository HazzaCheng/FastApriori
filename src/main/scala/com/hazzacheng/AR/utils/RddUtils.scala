package com.hazzacheng.AR.utils

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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
    val dataRDD = sc.textFile(path + "part-00000", sc.defaultParallelism * 4).map(_.split(" "))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.split(" "))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
  }

  def formatOutput(freqItems: Array[FPGrowth.FreqItemset[String]],
                   total: Long): Array[String] = {
    val strs = freqItems.map{x =>
      val percent = x.freq.toDouble / total
      val str = x.items.mkString("[", ",", "]")

      str + "(" + x.freq + " " + percent + ")"
    }

    strs
  }
}
