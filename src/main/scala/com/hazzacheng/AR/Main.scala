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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val sc = new SparkContext()
    val minSupport = 0.092
    val input = args(0)
    val output = args(1)
    val (dataRDD, userRDD) = utils.RddUtils.readAsRDD(sc, input)
//    val (freqItemsets, itemToRank) =
    new NFPGrowth(minSupport, 298).run(sc, dataRDD)
//    utils.RddUtils.formattedSave(sc, output, freqItemsets, itemToRank)
  }
}

