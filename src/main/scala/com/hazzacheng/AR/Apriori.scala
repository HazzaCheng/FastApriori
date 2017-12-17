package com.hazzacheng.AR

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-17 
  * Time: 10:12 AM
  */
class Apriori(private var minSupport: Double, private var numPartitions: Int) extends Serializable {
  val nums = 40


  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  def run(sc: SparkContext, data: RDD[Array[String]], output: String) = {
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)

    val count = data.count()
    var minCount = math.ceil(minSupport * count).toInt
    val (freqItems, itemToRank, newData, countMap, totalCount) = genFreqItems(sc, data, minCount, partitioner)
    minCount = math.ceil(minSupport * totalCount).toInt
    val freqItemsets = genFreqItemsets(sc, newData, countMap, totalCount, minCount, freqItems)

    val time = System.currentTimeMillis()
    val temp = freqItemsets.map{case (freqItemset, count) =>
      freqItemset.toArray.sorted.map(freqItems(_)).mkString(" ") + " [" + count + "]"
    }
    println("==== Use Time change to string " + (System.currentTimeMillis() - time))
    sc.parallelize(temp).saveAsTextFile(output)
  }

  private def genFreqItems(
                            sc: SparkContext,
                            data: RDD[Array[String]],
                            minCount: Long,
                            partitioner: Partitioner
                          ):(Array[String], mutable.HashMap[String, Int], RDD[(Int, Array[Int])], collection.Map[Int, Int], Int) = {

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
    val temp = data.map{x =>
      val freqItems = freqItemsBV.value
      val itemToRank = itemToRankBV.value
      (x.filter(freqItems.contains).map(itemToRank).toSet, 1)
    }.filter {
      case (transcation, count) =>
        transcation.size > 1 && transcation.size < 200
    }
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

    (freqItems, itemToRank, newData, countMap, totalCount)
  }

  private def genFreqItemsets(
                               sc: SparkContext,
                               newData: RDD[(Int, Array[Int])],
                               countMap: collection.Map[Int, Int],
                               totalCount: Int,
                               minCount: Int,
                               freqItems: Array[String]
                             ) = {
    val freqItemsSize = freqItems.length
    val freqItemsTrans = getFreqItemsTrans(newData, freqItems, totalCount)
    val freqItemsBV = sc.broadcast(freqItems)
    val countMapBV = sc.broadcast(countMap)
    val freqItemsTransBV = sc.broadcast(freqItemsTrans.toMap)
    val freqItemsets = mutable.ListBuffer.empty[(Set[Int], Int)]

    var time = System.currentTimeMillis()
    var kItemsetRDD = genTwoFreqItems(sc, countMapBV, freqItemsTransBV, freqItemsSize, totalCount, minCount)
    val tuplesWithCount = kItemsetRDD.map(x => (x._1, x._3)).collect()
    freqItemsets ++= tuplesWithCount
    var kItems = tuplesWithCount.map(_._1)
    println("==== 2 freq items " + kItems.length)
    println("==== Use Time 2 items " + (System.currentTimeMillis() - time))

    var k = 3
    while (kItems.length >= k) {
      time = System.currentTimeMillis()
     /* val candidates = genCandidates(sc, kItems, freqItemsSize)
      println("==== " + k + " candidate items " + candidates.length)*/
      val kItemsBV = sc.broadcast(kItems)
      val temp = genNextFreqItemsets(kItemsetRDD, countMapBV, freqItemsTransBV, kItemsBV, freqItemsSize, totalCount, minCount)
      val kItemsWithCount = temp.map(x => (x._1, x._3)).collect()
      freqItemsets ++= kItemsWithCount
      kItems = kItemsWithCount.map(_._1)
      kItemsBV.unpersist()
      kItemsetRDD.unpersist()
      kItemsetRDD = temp
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

  private def genNextFreqItemsets(
                                   kItemsetRDD: RDD[(Set[Int], Array[Boolean], Int)],
                                   countMapBV: Broadcast[collection.Map[Int, Int]],
                                   freqItemsTransBV: Broadcast[Map[Int, Array[Boolean]]],
                                   kItemsBV: Broadcast[Array[Set[Int]]],
                                   freqItemsSize: Int,
                                   totalCount: Int,
                                   minCount: Int
                                 ): RDD[(Set[Int], Array[Boolean], Int)] = {
    val res = kItemsetRDD.flatMap { case (kItems, line, count) =>
      val kItemsSet = kItemsBV.value
      val candidates = mutable.HashSet.empty[Int]
      Range(kItems.max + 1, freqItemsSize).foreach(candidates.add)
      candidates --= kItems
      val temp = kItems.toArray
      val len = temp.length
      var i = 0
      while (candidates.nonEmpty && i < len) {
        val subSet = kItems - temp(i)
        candidates.toArray.foreach { i =>
          if (!kItemsSet.contains(subSet + i))
            candidates -= i
        }
        i += 1
      }
      val kPlusOneItemset = candidates.toArray.map { i =>
        val countMap = countMapBV.value
        val freqItemsTrans = freqItemsTransBV.value
        val iLine = freqItemsTrans(i)
        val temp = new Array[Boolean](totalCount)
        val indexes = Range(0, totalCount).filter { i =>
          temp(i) = iLine(i) && line(i)
          iLine(i) && line(i)
        }.toArray
        var iCount = 0
        indexes.foreach(iCount += countMap(_))
        if (count >= minCount) (kItems + i, temp, iCount)
        else (Set.empty[Int], Array.empty[Boolean], 0)
      }.filter(_._3 != 0)

      kPlusOneItemset
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

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
                           ): Array[Set[Int]] = {
    val kItemsSetBV = sc.broadcast(kItems.toSet)
    val candidates = sc.parallelize(kItems, sc.defaultParallelism * nums * 10).flatMap{x =>
//      val time = System.currentTimeMillis()
      val kItemsSet = kItemsSetBV.value
      val candidates = mutable.HashSet.empty[Int]
      Range(0, freqItemsSize).foreach(candidates.add)
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
//      println("==== Use Time" + (System.currentTimeMillis() - time) + " " + x)
      candidates.toArray.map(x + _)
    }.collect().distinct

    kItemsSetBV.unpersist()

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
                             ): RDD[(Set[Int], Array[Boolean], Int)] = {
    val tuples = mutable.ListBuffer.empty[(Int, Int)]

    for (i <- 0 until freqItemsSize - 1)
      for (j <- i + 1 until freqItemsSize)
        tuples.append((i, j))

    println("==== 2 candidates items " + tuples.length)

    val res = sc.parallelize(tuples.toList, sc.defaultParallelism * nums).map{t =>
      val countMap = countMapBV.value
      val freqItemsTrans = freqItemsTransBV.value
      val x = freqItemsTrans(t._1)
      val y = freqItemsTrans(t._2)
      val line = new Array[Boolean](totalCount)
      val indexes = Range(0, totalCount).filter{i =>
        line(i) = x(i) && y(i)
        x(i) && y(i)
      }.toArray
      var count = 0
      indexes.foreach(count += countMap(_))
      if (count >= minCount) (Set[Int](t._1, t._2), line, count)
      else (Set.empty[Int], Array.empty[Boolean], 0)
    }.filter(_._3 != 0).persist(StorageLevel.MEMORY_AND_DISK_SER)

    res
  }

  private def arrayContains(transaction: Array[Int], item: Int): Boolean = {
    transaction.foreach(x => if (x == item) return true)
    false
  }

}
