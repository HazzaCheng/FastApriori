package com.hazzacheng.AR

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-17 
  * Time: 10:12 AM
  */
class FastApriori(private var minSupport: Double, private var numPartitions: Int) extends Serializable {


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
         ): (Array[(Set[Int], Int)], mutable.HashMap[String, Int], Array[String]) = {
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)

    val count = data.count()
    val minCount = math.ceil(minSupport * count).toInt
    val (freqItemsWithCount, freqItems, itemToRank, newData, countMap, totalCount) = genFreqItems(sc, data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(sc, newData, countMap, totalCount, minCount, freqItems) ++ freqItemsWithCount

    (freqItemsets, itemToRank, freqItems)
  }

  private def genFreqItems(sc: SparkContext,
                            data: RDD[Array[String]],
                            minCount: Long,
                            partitioner: Partitioner
                          ):(Array[(Set[Int], Int)], Array[String], mutable.HashMap[String, Int], RDD[(Int, Array[Int])], collection.Map[Int, Int], Int) = {

    val freqItemsSet = mutable.HashSet.empty[String]
    val itemToRank = mutable.HashMap.empty[String, Int]

    val tmp = data.flatMap(_.map((_, 1)))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()

    val freqItems = tmp.sortBy(-_._2).map(_._1)
    freqItems.foreach(freqItemsSet.add)
    freqItems.zipWithIndex.foreach(x => itemToRank.put(x._1, x._2))

    val freqItemsBV = sc.broadcast(freqItemsSet)
    val itemToRankBV = sc.broadcast(itemToRank)
    val temp = data.map{x =>
      val freqItems = freqItemsBV.value
      val itemToRank = itemToRankBV.value
      (x.filter(freqItems.contains).map(itemToRank).toSet, 1)
    }.filter(x => x._1.size > 1)
      .reduceByKey(_ + _)
      .map(x => (x._1.toArray, x._2))
      .zipWithIndex()
      .map(x => (x._2.toInt, x._1))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val countMap = temp.map(x => (x._1, x._2._2)).collectAsMap()
    val newData = temp.map(x => (x._1, x._2._1)).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val totalCount = newData.count().toInt
    data.unpersist()
    temp.unpersist()

    val freqItemsWithCount = tmp.map(x => (Set[Int](itemToRank(x._1)), x._2))

    (freqItemsWithCount, freqItems, itemToRank, newData, countMap, totalCount)
  }

  private def genFreqItemsets(
                               sc: SparkContext,
                               newData: RDD[(Int, Array[Int])],
                               countMap: collection.Map[Int, Int],
                               totalCount: Int,
                               minCount: Int,
                               freqItems: Array[String]
                             ): Array[(Set[Int], Int)] = {
    val freqItemsSize = freqItems.length
    val freqItemsTrans = getFreqItemsTrans(newData, freqItems, totalCount)
    val freqItemsBV = sc.broadcast(freqItems)
    val countMapBV = sc.broadcast(countMap)
    val freqItemsTransBV = sc.broadcast(freqItemsTrans.toMap)
    val freqItemsets = mutable.ListBuffer.empty[(Set[Int], Int)]

    var time = System.currentTimeMillis()
    val tuplesWithCount = genTwoFreqItems(sc, countMapBV, freqItemsTransBV, freqItemsSize, totalCount, minCount)
    freqItemsets ++= tuplesWithCount
    var kItems = tuplesWithCount.map(_._1)
    println("==== 2 freq items " + kItems.length)
    println("==== Use Time 2 items " + (System.currentTimeMillis() - time))

    var k = 3
    while (kItems.length >= k) {
      time = System.currentTimeMillis()
      val candidates = genCandidates(sc, kItems, freqItemsSize)
      println("==== " + k + " candidate items " + candidates.length)
      val kItemsWithCount = genNextFreqItemsets(sc, candidates, countMapBV, freqItemsTransBV, freqItemsSize, totalCount, minCount)
      freqItemsets ++= kItemsWithCount
      kItems = kItemsWithCount.map(_._1)
      println("==== " + k + " freq items " + kItems.length)
      println("==== Use Time " + k + " items " + (System.currentTimeMillis() - time))
      k += 1
    }

    freqItemsBV.unpersist()
    countMapBV.unpersist()
    freqItemsTransBV.unpersist()

    println("==== Total freq items sets " + freqItemsets.toList.length)

    freqItemsets.toArray
  }

  private def genNextFreqItemsets(sc: SparkContext,
                                  candidates: Array[(Set[Int], Array[Int])],
                                  countMapBV: Broadcast[collection.Map[Int, Int]],
                                  freqItemsTransBV: Broadcast[Map[Int, Array[Boolean]]],
                                  freqItemsSize: Int,
                                  totalCount: Int,
                                  minCount: Int): Array[(Set[Int], Int)] = {

    val res = sc.parallelize(candidates).flatMap { case (subSet, items) =>
      val countMap = countMapBV.value
      val freqItemsTrans = freqItemsTransBV.value
      val common = subSet.toArray.map(freqItemsTrans(_))
      val commonArray = new Array[Boolean](totalCount)
      Range(0, totalCount).foreach(i => commonArray(i) = logicalAnd(i, common))

      val right = items.map { i =>
        val iArray = freqItemsTrans(i)
        val indexes = Range(0, totalCount).filter(x => commonArray(x) && iArray(x))
        var count = 0
        indexes.foreach(count += countMap(_))
        if (count >= minCount) (subSet + i, count)
        else (Set.empty[Int], 0)
      }.filter(_._2 != 0)

      right
    }.collect()

    res
  }

  private def logicalAnd(index: Int, items: Array[Array[Boolean]]): Boolean = {
    items.foreach(x => if (!x(index)) return false)
    true
  }

  private def genCandidates(
                             sc: SparkContext,
                             kItems: Array[Set[Int]],
                             freqItemsSize: Int
                           ): Array[(Set[Int], Array[Int])] = {
    val kItemsSetBV = sc.broadcast(kItems.toSet)
    val candidates = sc.parallelize(kItems).map{ x =>
      val kItemsSet = kItemsSetBV.value
      val candidates = mutable.HashSet.empty[Int]
      Range(x.max + 1, freqItemsSize).foreach(candidates.add)
      candidates --= x
      val temp = x.toArray
      val len = temp.length
      var i = 0
      while (candidates.nonEmpty && i < len) {
        val subSet = x - temp(i)
        candidates.toArray.foreach{y =>
          if (!kItemsSet.contains(subSet + y))
            candidates -= y
        }
        i += 1
      }
      (x, candidates.toArray)
    }.filter(_._2.nonEmpty).collect()

    candidates
  }

  private def getFreqItemsTrans(newData: RDD[(Int, Array[Int])],
                                freqItems: Array[String],
                                totalCount: Int
                               ): Array[(Int, Array[Boolean])] = {
    val itemsWithTrans = freqItems.indices
      .map(x => (x, newData.filter(y => arrayContains(y._2, x)).map(_._1).collect()))
      .toArray
      .map{x =>
        val array = new Array[Boolean](totalCount)
        x._2.foreach(array(_) = true)
        (x._1, array)
      }
    newData.unpersist()

    itemsWithTrans
  }

  private def genTwoFreqItems(
                               sc: SparkContext,
                               countMapBV: Broadcast[collection.Map[Int, Int]],
                               freqItemsTransBV: Broadcast[Map[Int, Array[Boolean]]],
                               freqItemsSize: Int,
                               totalCount: Int,
                               minCount: Int
                             ): Array[(Set[Int], Int)] = {
    val tuples = mutable.ListBuffer.empty[(Int, Int)]

    for (i <- 0 until freqItemsSize - 1)
      for (j <- i + 1 until freqItemsSize)
        tuples.append((i, j))

    println("==== 2 candidates items " + tuples.length)

    val res = sc.parallelize(tuples.toList).map{t =>
      val countMap = countMapBV.value
      val freqItemsTrans = freqItemsTransBV.value
      val x = freqItemsTrans(t._1)
      val y = freqItemsTrans(t._2)
      val indexes = Range(0, totalCount).filter(i => x(i) && y(i)).toArray
      var count = 0
      indexes.foreach(count += countMap(_))
      if (count >= minCount) (Set[Int](t._1, t._2), count)
      else (Set.empty[Int], 0)
    }.filter(_._2 != 0).collect()

    res
  }

  private def arrayContains(transaction: Array[Int], item: Int): Boolean = {
    transaction.foreach(x => if (x == item) return true)
    false
  }

}
