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
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, mllib}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.mllib.fpm.AssociationRules
import java.io.{File, PrintWriter}

import org.apache.spark.mllib.fpm
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FPGrowthTest")//.set("spark.driver.maxResultSize","2g")
    //.set("spark.sql.warehouse.dir", "~/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val sc = new SparkContext(conf)




//    val data = sc.textFile(args(0), sc.defaultParallelism)
//    val K = 20
//    val Iteration = 50
//    val dataset = data.map { line =>
//      line.trim.split("\t").grouped(2).map(kv => kv(0).toInt -> kv(1).toDouble).toSeq
//    }
//    val vectorSize = dataset.map(_.map(_._1).max).max + 1
//    val vectors = dataset.map { xs =>
//      Vectors.sparse(vectorSize, xs)
//    }.cache
//
//    val clusters = mlKMeans.train(vectors, K, Iteration)
//    clusters.predict(vectors).saveAsTextFile(args(1))

    val minSupport = 0.092
    val minConfidence = 0.8
    //val sumCores = conf.getInt("spark.executor.cores", 4) * conf.getInt("spark.executor.instances", 12)
    //val partitioner = new HashPartitioner(sumCores * 2)
    val sumCores = sc.defaultParallelism * 32
    println(sumCores)

    //println("finish")

    //val data = sc.textFile("data/sample_fpgrowth.txt")

    val path1 = args(0)+"part-00000"
    val path2 = args(0)+"U.dat"
    val data_D = sc.textFile(path1, sumCores)
    val data_U = sc.textFile(path2, sumCores)
    val partitioner = new HashPartitioner(sumCores)
    val group = 5

    /*
    val data = sc.wholeTextFiles(args(0), sc.defaultParallelism * 4)
    val filename = data.keys.collect()
    val data_D = data.filter(file => file._1.equals(filename(0))).values
    val data_U = data.filter(file => file._1.equals(filename(1))).values
    */

    //val transactions = sc.textFile(args(0)).map(_.split(" ")).cache()
    //Kmeans.kmeans(data_D,sumCores,args(1))
    //Apriori.run(sc, args(0), args(1),minSupport)


//    val len = data_D.map(_.split(" "))
//      .map(line => line.length)
//      .map(l => (l, 1L))
//      .reduceByKey(_+_).repartition(1).sortBy(_._1)
//    len.saveAsTextFile(args(1))



    val pro_data =Preprocess.prepro(sc,data_D,sumCores,minSupport,args(1))
    //Kmeans.kmeans(pro_data,sumCores,args(1))

    run_FP.run_FP(minSupport,sumCores,group,pro_data,args(1))
    println("partx20")

    //Apriori.run(sc, path1, args(1),minSupport)


    //    val total = data_D.map(x => x.split(" ")).map(x=>(x.length,x))
//    val transactions = total.groupByKey().cache()
//
//    transactions.map(x=> fprun(x,sc))
//    print(transactions.partitions.mkString(" "))


    //val temp = transactions.collect()
    //    val dataSize = data_D.count()
    //    val minSupport = (dataSize * supportThreshold).toLong
    //        val temp = transactions.collect()
    //        for (i <- temp)
    //          println(i)


//        val dataset = data_D.map(line => line.split('\n'))
//          .flatMap(item=>item).repartition(sumCores)
//        val transactions = dataset.map(x => x.split(" ")).cache()
//        transactions.first().foreach(println)


//        val part_trans = transactions.map(
//          x=> (x.length/10,List(x.toList))
//        ).reduceByKey(_:::_)


        //val transactions = data_D.map(_.split(" ")).cache()





/*


        val model = fpg.run(transactions)

        //val model = part_trans.map(transsations => fpg.run(transactions))

        println("yyyyyyyy")

/*
        val rjk = model.generateAssociationRules(minConfidence)
        //val rjk = model.map(cluster => cluster.generateAssociationRules(minConfidence))

        println("partx1")


        val dataUset = data_U.map(line => line.split('\n')).flatMap(item=>item).repartition(sumCores)
        println("partx2")
        val result = Match.match_U(sc,dataUset, model, minConfidence, rjk)
        val last =  result.map(
          rule => (rule._1.antecedent.mkString(""), List(List((rule._1.consequent, rule._2)))))

        println("partx10")
        val temp =last.reduceByKey(
          _ ::: _)

        println("partx9")
        val t =  temp.map(
            item => (item._1, item._2)
          ).map(
          res => {
            val s = res._1.mkString(",") + "-----" + res._2.head + "------" + res._2.head
            s
          }
        )

        println("partx3")
        //t.repartition(sumCores).saveAsTextFile(args(1))

*/

        //查看所有的频繁项集，并且列出它出现的次数
        val fre = model.freqItemsets
          .map(itemset => {
          val s= itemset.items.mkString("[", ",", "]") + "," + itemset.freq
          s
        })

        println("partx4")


        //fre.saveAsTextFile(args(1))

        println("partx5")


        val freqItemsets =model.freqItemsets

        val ar = new AssociationRules()

        val results = ar
          .setMinConfidence(0.8)
          .run(model.freqItemsets)
          .collect()

*/

/*
        val parts=model.freqItemsets.partitions
        print(parts.length+"================")
        val fre=parts.map(p=>{
          val idx1 =p.index
          val partRdd1 =model.freqItemsets.mapPartitionsWithIndex{
            case (index,value)=>
              if (index == idx1) value
              else Iterator()}
          val dataPartitioned = partRdd1.collect().foreach(
            itemset =>itemset.items.mkString("[", ",", "]") + "," + itemset.freq
            )
          p
        })

*/





    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //model.generateAssociationRules(minConfidence).collect().foreach(rule => {
    //  println(rule.antecedent.mkString(",") + "-->" +
    //    rule.consequent.mkString(",") + "-->" + rule.confidence)
    //})

    //println(model.freqItemsets
    // .filter( rule => rule.items.length !=1)
    // .collect().map(item => item.items.length).sum)

    //查看规则生成的数量
    //println(model.generateAssociationRules(minConfidence).collect().length)


    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

//        println("part3")
//
//        val patterns = ParallelFPGrowth(sc, data_D,
//          (data_D.count()*minSupport).toLong, " ", sumCores)
//        // Print results on the terminal.
//        println("part4")
//        var count: Int = 0
//        for (pattern <- patterns.collect) {
//          println(pattern._1 + " " + pattern._2)
//          count += 1
//        }
//        println("---------------------------------------------------------")
//        println("count = " + count)
//        println("---------------------------------------------------------")
//
//        //Write elements of patterns as text files in a given directory in hdfs.
//        patterns.saveAsTextFile(args(1))

    // =============parallize================

//    val firdd = transactions.mapPartitions(p => fprun(p, sc))
//
//    val bro_firdd = sc.broadcast(firdd)
//
//    val supp_item = firdd.mapPartitions(p => getFrequet(p,sc))
  }

//  def fprun[T](iter: Iterator[T], sc: SparkContext): Iterator[(T, T)] = {
//    val minSupport = 0.092
//    val minConfidence = 0.8
//    val sumCores = 6
//    val fpg = new FPGrowth()
//      .setMinSupport(minSupport)
//      .setNumPartitions(sumCores)
//    var res = List[(T, T)]()
//    var model = fpg.run(sc.parallelize(iter.toList))
//    res.iterator
//  }

//
//
//  def getFrequet[T](iter: Iterator[T], sc: SparkContext): Iterator[(T, T)] = {
//    val minSupport = 0.092
//    val minConfidence = 0.8
//    val sumCores = 6
//    val fpg = new FPGrowth()
//      .setMinSupport(minSupport)
//      .setNumPartitions(sumCores)
//    var res = List[(T, T)]()
//    var model = fpg.run(sc.parallelize(iter.toList))
//    res.iterator
//  }


}

