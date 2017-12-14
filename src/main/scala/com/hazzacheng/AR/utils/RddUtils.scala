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
    val dataRDD = sc.textFile(path + "D.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val userRDD = sc.textFile(path + "U.dat", sc.defaultParallelism * 4).map(_.trim().split("\\s+"))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (dataRDD, userRDD)
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
                    freqItems: List[Set[String]],
                    oneItemCountMap: Map[String, Int]): Unit = {
    val strs = freqItems.map(_.toList.sortBy(oneItemCountMap(_)).mkString(" "))
    sc.parallelize(strs).saveAsTextFile(path)
  }

  def removeRedundancy(sc: SparkContext,
                       data: RDD[Array[String]],
                       min: Int
                      ): (RDD[(Set[String], Long)], Array[String], Array[Int], Map[String, Int]) = {
    val itemsSet = mutable.HashSet.empty[String]
    val itemsOrders = mutable.ListBuffer.empty[(String, Int)]

    data.flatMap(_.map((_, 1)))
      .reduceByKey(_ + _)
      .filter(_._2 >= min).collect()
      .foreach { x =>
        itemsSet.add(x._1)
        itemsOrders.append((x._1, x._2))
      }
    val oneItemset = itemsOrders.toList.sortBy(_._2).map(_._1).toArray
    val oneItemCountMap = itemsOrders.toMap

    val size = itemsSet.size
    println("==== items count: " + size)

    val itemsBV = sc.broadcast(itemsSet)
    val nonDuplicates = data.map(x => (x.filter(itemsBV.value.contains(_)).toSet, 1))
      .filter(x => x._1.size > 1 && x._1.size < 100)
      .reduceByKey(_ + _)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val countArr = nonDuplicates.map(x => (x._2, x._1._2)).collect().sortBy(_._1).map(_._2)
    val newRdd = nonDuplicates.map(x => (x._1._1, x._2)).persist(StorageLevel.MEMORY_AND_DISK_SER)

    (newRdd, oneItemset, countArr, oneItemCountMap)
  }

  def getFreqOneItemset(rdd: RDD[(Set[String], Long)],
                        oneItemset: Array[String],
                        transLen: Int
                       ): Map[String, Set[Int]] = {
    val oneItemMap = oneItemset.map{x =>
      val indexes = rdd.filter(_._1.contains(x)).map(_._2.toInt).collect()
      (x, indexes.toSet)
    }.toMap

    oneItemMap
  }


}
