package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, Partitioner, SparkException,SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object Preprocess {
  def prepro(sc: SparkContext,data: RDD[String],sumCores:Int,minSupport :Double,outputPath: String): RDD[Array[String]]= {


    //val datasetx = data.repartition(sumCores).map(line=>line.split('\n'))
    val dataset = data.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)
    val transactions = dataset.map(x => x.split(" "))

    val count = transactions.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numPartitions = sumCores
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)

    val dropele =transactions.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 < minCount)
      //.collect()
      .flatMap(_._1).map(i =>i.toString)

      val set_drop = dropele.collect().toSet
      val broad_drop = sc.broadcast(set_drop)

//    val temp = dropele.collect()
//    temp.foreach(println)

    val data_press = transactions.map(item =>
      item.toSet -- broad_drop.value
      ).map(item=>item.toArray)
    //data_press.saveAsTextFile(outputPath)

    data_press

  }
}
