package com.hazzacheng.AR

/**
  * Created with IntelliJ IDEA.
  *
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 2017-09-26
  * Time: 10:04 PM
  */
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.default.parallelism", "300")
      .set("spark.speculation", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.getAll.foreach(x => println("==== " + x))

    val sc = new SparkContext(conf)
    val minSupport = 0.092
    val input = args(0)
    val output = args(1)
    val (dataRDD, userRDD) = Utils.readAsRDD(sc, input)

    val (freqItemsets, itemToRank, freqItems) =
      new FastApriori(minSupport, sc.defaultParallelism).run(sc, dataRDD)
    Utils.saveFreqItemsetWithCount(sc, output, freqItemsets, freqItems)

//    val (freqItemsetTP, itemToRankTP, freqItemsTP) = Utils.getAll(sc)
//    new AssociationRules(freqItemsetTP, freqItemsTP, itemToRankTP).run(sc, userRDD)
    val recommends = new AssociationRules(freqItemsets, freqItems, itemToRank).run(sc, userRDD)
    Utils.saveRecommends(sc, output, recommends)
  }
}

