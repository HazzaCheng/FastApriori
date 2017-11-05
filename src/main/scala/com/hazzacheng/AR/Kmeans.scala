package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans => mlKMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object Kmeans {

  def kmeans(data: RDD[String],sumCores:Int,outputPath: String): RDD[Int] = {

    val K = 10
    val Iteration = 20

//    val dataset = data.map ( line =>
//      Vectors.dense(line.split('\n').map(_.toDouble))
//    ).cache
    val dataset = data.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)

    //val datarep = dataset.map(line =>Vectors.dense(line.split(' ').map(_.toDouble))).cache

//    val datarep = dataset.map { line =>
//      line.trim.split(" ").grouped(2).map(kv => kv(0).toInt -> kv(1).toDouble).toSeq
//    }

    val datacal = dataset.map(line=> {
      val temp = line.trim.split(" ")
      (temp.map(_.toInt), temp.map(_.toDouble))
    })

    val vectorSize = datacal.map(line => line._1.max).max+1



    val vectors = datacal.map { xs =>
      Vectors.sparse(vectorSize,xs._1,xs._2)
    }.cache

    //println("xxxxx"+datarep.first()(1))



//    val vectorSize = dataset.map(_.map(_._1).max).max + 1
//    val vectors = dataset.map { xs =>
//      Vectors.sparse(vectorSize, xs)
//    }.cache
    //val datarep = dataset.repartition(200)
    val clusters = mlKMeans.train(vectors, K, Iteration)
    println("llllllll")
    val preres = clusters.predict(vectors)

    println("ttttttt")
    clusters.clusterCenters.foreach(x=>println(x+"xxxxxxxxxxxx"))
    preres.saveAsTextFile(outputPath)
    println("ooooooooo")
    preres

  }

}
