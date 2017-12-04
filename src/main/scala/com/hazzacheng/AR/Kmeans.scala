package com.hazzacheng.AR

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans => mlKMeans}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel

object Kmeans {

  def kmeans(data: RDD[Array[String]],sumCores:Int,outputPath: String):  RDD[(Int, List[Array[String]])] = {

    val K = 6
    val Iteration = 20
    val partitioner = new HashPartitioner(sumCores)
    val D =data.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val dataset = data.map(line => (line.map(_.toInt),line.map(_ => 1.0)))

//    val dataset = data.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)
//
//    val datacal = dataset.map(line=> {
//      val temp = line.trim.split(" ")
//      (temp.map(_.toInt), temp.map(_.toDouble))
//    })

    val vectorSize = dataset.map(line => line._1.max).max+1

    //println(vectorSize+"partxx")
    val index = 0 to vectorSize toArray
    val vectors = dataset.map { xs =>
      Vectors.sparse(vectorSize,xs._1,xs._2)
    }

//    val vectorSize = dataset.map(_.map(_._1).max).max + 1
//    val vectors = dataset.map { xs =>
//      Vectors.sparse(vectorSize, xs)
//    }.cache

    val clusters = mlKMeans.train(vectors, K, Iteration)
    val preres = clusters.predict(vectors)
    val cluster_num = preres.collect()


    clusters.clusterCenters.foreach(x=>println(x+"xxxxxxxxxxxx"))
    //preres.saveAsTextFile(outputPath)
    println("ooooooooo")

    val pre = D.zipWithIndex().map(x => (cluster_num(x._2.toInt),List(x._1)))
      .reduceByKey(partitioner,_:::_)


//    pre.map(x => {
//      val  num = x._1.toString
//      x._2.map(x =>num +"xxxxxxxxxxx"+ x.mkString(" "))
//    }).saveAsTextFile(outputPath)
    println(preres.count+"xxxxxxxxxxxxxxxxxx")
    pre

  }

}
