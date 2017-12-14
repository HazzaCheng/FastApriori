package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, Partitioner, SparkContext, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.FPGrowthModel.SaveLoadV1_0.getClass
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowthModel, FPTree}
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-14 
  * Time: 10:03 PM
  */

class NFPGrowth (private var minSupport: Double,
                 private var numPartitions: Int) extends Serializable {

  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(sc: SparkContext, data: RDD[Array[String]]): FPGrowthModel[String] = {
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val (freqItems, freqItemsMap, newData) = genFreqItems(sc, data, minCount, partitioner)
    data.unpersist()
    val newFreqItems = freqItems.indices.toArray
    val newCount = newData.count()
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new FPGrowthModel(freqItemsets)
  }

  private def genFreqItems(
                            sc: SparkContext,
                            data: RDD[Array[String]],
                            minCount: Long,
                            partitioner: Partitioner
                          ):(Array[String], mutable.HashMap[String, Int], RDD[Array[Int]]) = {

    val freqItemsSet = mutable.HashSet.empty[String]
    val freqItemsMap = mutable.HashMap.empty[String, Int]

    val freqItems = data.flatMap(_.map((_, 1)))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
    freqItems.foreach(freqItemsSet.add(_))
    freqItems.zipWithIndex.foreach(x => freqItemsMap.put(x._1, x._2))

    val freqItemsBV = sc.broadcast(freqItemsSet)
    val freqItemsMapBV = sc.broadcast(freqItemsMap)
    val newData = data.map{x =>
      val freqItems = freqItemsBV.value
      val freqItemsMap = freqItemsMapBV.value
      x.filter(freqItems.contains(_)).map(freqItemsMap(_)).sortBy(-_)
    }
      .filter(_.length > 1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (freqItems, freqItemsMap, newData)
  }

  private def genFreqItemsets(
                               data: RDD[Array[String]],
                               minCount: Long,
                               freqItems: Array[String],
                               partitioner: Partitioner): RDD[FreqItemset[Int]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

}
