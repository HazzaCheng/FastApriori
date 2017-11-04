package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans => mlKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object Kmeans {

  def kmeans(data: RDD[String],sumCores:Int,outputPath: String): RDD[Int] = {

    val K = 20
    val Iteration = 50

//    val dataset = data.map ( line =>
//      Vectors.dense(line.split('\n').map(_.toDouble))
//    ).cache
    val dataset = data.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)

    val datarep = dataset.map(line =>Vectors.dense(line.split(' ').map(_.toDouble))).cache

    //println("xxxxx"+datarep.first()(1))



//    val vectorSize = dataset.map(_.map(_._1).max).max + 1
//    val vectors = dataset.map { xs =>
//      Vectors.sparse(vectorSize, xs)
//    }.cache
    //val datarep = dataset.repartition(200)
    val clusters = mlKMeans.train(datarep, K, Iteration)
    val preres = clusters.predict(datarep)
    preres.repartition(50).saveAsTextFile(outputPath)
    preres
  }

}
