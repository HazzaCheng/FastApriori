package com.hazzacheng.AR

import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, mllib}

object run_FP {

  def run_FP(minSupport:Double,sumCores:Int,group:Int,
             dataset: RDD[Array[String]],outputpath:String): Unit ={

    val transactions = dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)

    var temp = transactions.filter(_.length%group==0)
    var fre = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(sumCores)
      .run(temp).
      freqItemsets

    for (i<-1 until group)
    {
      var temp = transactions.filter(_.length%group==i)
      var fre_temp = new FPGrowth()
        .setMinSupport(minSupport)
        .setNumPartitions(sumCores)
        .run(temp).
        freqItemsets
      fre = fre.union(fre_temp)
    }

    fre.repartition(sumCores).saveAsTextFile(outputpath)

  }

}
