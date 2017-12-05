package com.hazzacheng.AR.utils

import org.apache.spark.SparkContext
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
    val dataRDD = sc.textFile(path + "D.dat", sc.defaultParallelism * 4).map(_.split(" "))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.split(" "))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
  }
}
