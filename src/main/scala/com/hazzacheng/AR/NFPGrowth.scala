package com.hazzacheng.AR

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-14 
  * Time: 10:03 PM
  */

class NFPGrowth (private var minSupport: Double, private var numPartitions: Int) extends Serializable {

  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(
           sc: SparkContext,
           data: RDD[Array[String]]
         ): (RDD[(Array[String], Int)], mutable.HashMap[String, Int]) = {
    val count = data.count()
    var minCount = math.ceil(minSupport * count).toInt
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val (freqItems, itemToRank, newData) = genFreqItems(sc, data, minCount, partitioner)
    data.unpersist()
   // val newFreqItems = freqItems.indices.toArray
    val newCount = newData.count()
    minCount = math.ceil(minSupport * newCount).toInt
    val freqItemsets = genFreqItemsets(newData, minCount, freqItems, partitioner)

    (freqItemsets, itemToRank)
  }

  private def genFreqItems(
                            sc: SparkContext,
                            data: RDD[Array[String]],
                            minCount: Long,
                            partitioner: Partitioner
                          ):(Array[String], mutable.HashMap[String, Int], RDD[Array[Int]]) = {

    val freqItemsSet = mutable.HashSet.empty[String]
    val itemToRank = mutable.HashMap.empty[String, Int]

    val freqItems = data.flatMap(_.map((_, 1)))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
    freqItems.foreach(freqItemsSet.add)
    freqItems.zipWithIndex.foreach(x => itemToRank.put(x._1, x._2))

    val freqItemsBV = sc.broadcast(freqItemsSet)
    val itemToRankBV = sc.broadcast(itemToRank)
    val newData = data.map{x =>
      val freqItems = freqItemsBV.value
      val itemToRank = itemToRankBV.value
      x.filter(freqItems.contains).map(itemToRank).sorted
    }
      .filter(_.length > 1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    (freqItems, itemToRank, newData)
  }

  private def genFreqItemsets(
                               data: RDD[Array[Int]],
                               minCount: Int,
                               freqItems: Array[String],
                               partitioner: Partitioner
                             ): RDD[(Array[String], Int)] = {
    val freqItemsets = data.flatMap { transaction =>
      genCondTransactions(transaction, partitioner)
    }.aggregateByKey(new FPTree, partitioner.numPartitions)(
        (tree, transaction) => tree.add(transaction, 1),
        (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      (ranks.map(i => freqItems(i)).toArray, count)
    }

    freqItemsets
  }

  private def genCondTransactions(
                                   transaction: Array[Int],
                                   partitioner: Partitioner
                                 ): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    var i = transaction.length - 1
    while (i >= 0) {
      val item = transaction(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) output(part) = transaction.slice(0, i + 1)
      i -= 1
    }
    output
  }

}
