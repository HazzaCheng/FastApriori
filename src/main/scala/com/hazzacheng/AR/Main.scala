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
//      .set("spark.rdd.compress", "true")
      .set("spark.speculation", "true")
      .set("spark.default.parallelism", "180")
//      .set("spark.shuffle.compress", "true")
//      .set("spark.shuffle.memoryFraction", "0.3")
//      .set("spark.storage.memoryFraction", "0.5")
    conf.getAll.foreach(x => println("==== " + x))

    val sc = new SparkContext(conf)
    val minSupport = 0.092
    val input = args(0)
    val output = args(1)
    val (dataRDD, userRDD) = Utils.readAsRDD(sc, input)
  /*
    val (freqItemsets, itemToRank, freqItems) =
      new FastApriori(minSupport, sc.defaultParallelism).run(sc, dataRDD)
    Utils.saveFreqItemset(sc, output, freqItemsets, freqItems)
*/
    val (freqItemsetTP, itemToRankTP, freqItemsTP) = Utils.getAll(sc)
    new AssociationRules(freqItemsetTP, freqItemsTP, itemToRankTP).run(sc, userRDD)
  }
}

