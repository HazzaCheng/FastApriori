package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

object Preprocess {
  def prepro(sc: SparkContext,data: RDD[String],sumCores:Int,minSupport :Double,outputPath: String): RDD[Array[String]]= {


    //val datasetx = data.repartition(sumCores).map(line=>line.split('\n'))
//    val dataset = data.flatMap(line => line.split('\n'))//.flatMap(item=>item).repartition(sumCores)
//    val transactions = dataset.map(x => x.split(" ")).repartition(sumCores).cache()

    val transactions = data.map(_.split(" ")).persist(StorageLevel.MEMORY_AND_DISK_SER)
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
      .filter(_._2 > minCount)
      .map(x=>x._1)

      val set_drop = dropele.collect().toSet

      val broad_drop = sc.broadcast(set_drop)

    val data_press = transactions.map(item =>{
      val temp =item.toSet.intersect(broad_drop.value)//--broad_drop_odd.value
      temp
    }
    ).map(v=> (v, 1L))
      .reduceByKey(partitioner,_+_)
      .filter(_._2==1)
      .map(x=>x._1).map(x => (x,x.size))
      .filter(_._2 <= 57)
      .map(x =>x._1)//.sortBy(_.head)
      .map(item=>item.toArray)


    //dropele.repartition(1).saveAsTextFile(outputPath)

    //data_press.map(x => x.mkString(" ")).repartition(1).saveAsTextFile(outputPath)


    data_press

  }
}
