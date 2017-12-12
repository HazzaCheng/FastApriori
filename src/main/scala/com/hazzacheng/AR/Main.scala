package com.hazzacheng.AR

import com.hazzacheng.AR.utils.RddUtils
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created with IntelliJ IDEA.
  * Description: 
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-4 
  * Time: 8:35 PM
  */
object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.Kryoserializer.buffer.max","1024m")
    val sc = new SparkContext(conf)
    val input = args(0)
    val output = args(1)
    val minSupport = 0.092
    val (dataRDD, userRDD) = RddUtils.readAsRDD(sc, input)
    ARsMine.findOnSpark(sc, output, dataRDD, userRDD, minSupport)

  }
}
