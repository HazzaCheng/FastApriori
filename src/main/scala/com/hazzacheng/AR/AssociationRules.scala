package com.hazzacheng.AR

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-18 
  * Time: 9:01 PM
  */
class AssociationRules(
                        private val freqItemset: Array[(Set[Int], Int)]
                      ) extends Serializable {

  def run(
           sc: SparkContext,
           userRDD: RDD[Array[String]],
           itemToRank: mutable.HashMap[String, Int],
           freqItems: Array[String]
         ) = {
    val (newRDD, empty) = removeRedundancy(sc, userRDD, itemToRank, freqItems)
    val recommends = (genAssociationRules(sc, newRDD, freqItems) ++ empty).sortBy(_._1)

    sc.parallelize(recommends).repartition(1).map(x => x._1 + " " + x._2).saveAsTextFile("/user_res")

    //newRDD.map(_._1.mkString(" ")).saveAsTextFile("/user_res")
  }

  def removeRedundancy(
                        sc: SparkContext,
                        userRDD: RDD[Array[String]],
                        itemToRank: mutable.HashMap[String, Int],
                        freqItems: Array[String]
                      ): (RDD[(Set[Int], Int)], Array[(Int, String)]) = {
    val items = mutable.HashSet.empty[String]
    items ++= freqItems
    val itemsBV = sc.broadcast(items)
    val itemToRankBV = sc.broadcast(itemToRank)

    val temp = userRDD.zipWithIndex().map{ case (line, index) =>
      val items = itemsBV.value
      val itemToRank = itemToRankBV.value
      val filtered = line.filter(items.contains).map(itemToRank).toSet
      (filtered, index.toInt)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val empty = temp.filter(_._1.isEmpty).map(x => (x._2, "0")).collect()
    val newRDD = temp.filter(_._1.nonEmpty).persist(StorageLevel.MEMORY_AND_DISK_SER)

    itemsBV.unpersist()
    itemToRankBV.unpersist()

    (newRDD, empty)
  }

  def genAssociationRules(
                           sc: SparkContext,
                           newRDD: RDD[(Set[Int], Int)],
                           freqItems: Array[String]
                         ): Array[(Int, String)] = {
    val time = System.currentTimeMillis()
    val grouped = freqItemset.groupBy(_._1.size)
    println("==== Use Time grouped " + (System.currentTimeMillis() - time))
    val subToSuper = genSuperSets(sc, grouped)
    val subToSuperBV = sc.broadcast(subToSuper)
    val freqItemsBV = sc.broadcast(freqItems)

    val recommends = newRDD.map { case (user, index) =>
      var recommend = -1
      var max: Double = 0.0
      val subToSuper = subToSuperBV.value
      val freqItems = freqItemsBV.value
      val userLen = user.size

      subToSuper.foreach { case (subset, comp) =>
        val subsetLen = subset.size
        if (userLen >= subsetLen && (subset -- user).isEmpty) {
          var flag = false
          var i = 0
          val len = comp.length
          while (!flag && i < len) {
            val tmp = comp(i)
            if (!user.contains(tmp._1)) {
              flag = true
              if (tmp._2 > max ||
                (tmp._2 == max && freqItems(tmp._1).toInt < freqItems(recommend).toInt)) {
                max = tmp._2
                recommend = tmp._1
              }
            }
            i += 1
          }
        }
      }

      if (recommend == -1) (index, "0")
      else (index, freqItems(recommend))
    }.collect()

    subToSuperBV.unpersist()
    freqItemsBV.unpersist()

    recommends
  }

  def genSuperSets(
                    sc: SparkContext,
                    grouped: Map[Int, Array[(Set[Int], Int)]]
                  ): Array[(Set[Int], Array[(Int, Double)])] = {
    val maxLen = grouped.keys.max
    val subsets = freqItemset.filter(_._1.size != maxLen)
    val freqItemsetBV = sc.broadcast(grouped)
    val subToSuper = sc.parallelize(subsets).map{ case (subset, count) =>
      val supersets = freqItemsetBV.value(subset.size + 1)
      val complements = mutable.ListBuffer.empty[(Int, Double)]
      supersets.foreach{ case (superset, sCount) =>
        val comp = superset -- subset
        if (comp.size == 1)
          complements.append((comp.head, sCount.toDouble / count))
      }
      (subset, complements.toArray.sortBy(-_._2))
    }.filter(_._2.nonEmpty).collect()

    freqItemsetBV.unpersist()

    subToSuper
  }


}
