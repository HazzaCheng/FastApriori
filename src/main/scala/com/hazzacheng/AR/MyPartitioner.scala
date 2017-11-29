package com.hazzacheng.AR

import org.apache.spark.Partitioner
import org.apache.spark.RangePartitioner

// 自定义Partitioner函数
class MyPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case iKey: Int => iKey % numPartitions
      case _ => 0
    }
  }

  override def equals(other: Any): Boolean = {
    other match {
      case h: MyPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
  }
}

