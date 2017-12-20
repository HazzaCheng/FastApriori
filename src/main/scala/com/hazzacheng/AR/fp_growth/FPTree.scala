package com.hazzacheng.AR.fp_growth

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA.
  * Description:
  * User: HazzaCheng
  * Contact: hazzacheng@gmail.com
  * Date: 17-12-14
  * Time: 10:03 PM
  */

class FPTree extends Serializable {

  import FPTree._

  val root: Node = new Node(null)
  val suffixItems = new mutable.ArrayBuffer[Int]()
  val allFreqItems = new mutable.ArrayBuffer[Array[Int]]

  private val summaries = mutable.Map.empty[Int, Summary]

  /** Adds a transaction with count. */
  def add(t: Iterable[Int], count: Int = 1): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree): this.type = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  private def project(suffix: Int): FPTree = {
    val tree = new FPTree
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[Int]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[Int], Int)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node): Iterator[(List[Int], Int)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
               minCount: Int,
               validateSuffix: Int => Boolean = _ => true
             ): Iterator[(List[Int], Int)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }

  def extract_imrpove(minCount: Int): Array[Array[Int]] = {
    val highItem = suffixItems.toArray.min
    val lowItem = suffixItems.toArray.max
    val highNode = summaries(highItem).nodes.toArray
    val count = summaries(lowItem).nodes.toArray
    mine(highNode, count, suffixItems.toArray, minCount)
    allFreqItems.toArray
  }

  def mine(node: Array[Node], count: Array[Node],
           curItems: Array[Int], minCount: Int):Unit = {
    val (itemsAndCount, itemsExistMap) = getItemsAndCount(node, count, minCount)
    val items = itemsAndCount.keys.toArray
    if(itemsAndCount.isEmpty)return
    if(itemsAndCount.size == 1){
      allFreqItems += curItems ++ items
      return
    }
    if(node.length == 1){
      // 生成任意组合
      genAllfreqItems(items, curItems)
      return
    }
    val (freqSingleItem, freqItems) = genKFreqItems(node, count, itemsAndCount, itemsExistMap, curItems, minCount)
    allFreqItems ++= freqItems
    for(i <- freqSingleItem){
      val highNode = summaries(i).nodes.toArray
      if(highNode.nonEmpty){
        mine(highNode, count, curItems ++ Array(i), minCount)
      }
    }
  }

  def genAllfreqItems(nums: Array[Int], curItems: Array[Int]): Unit = {
    val numsLen = nums.length
    val subsetLen = 1 << numsLen

    for (i <- 0 until subsetLen) {
      val subSet = mutable.Set.empty[Int]
      for (j <- 0 until numsLen) {
        if (((i >> j) & 1) != 0) subSet += nums(j)
      }
      if (subSet.nonEmpty) allFreqItems += subSet.toArray ++ curItems
    }

  }

  def genKFreqItems(node: Array[Node], count: Array[Node],
                    itemsAndCount: Map[Int, Int],
                    itemsExistMap: mutable.HashMap[Int, mutable.HashMap[Int, Boolean]],
                    curItems: Array[Int],
                    minCount: Int) = {
    val candidateItem = itemsAndCount.keys.toArray
    val candidateItemAndCount = mutable.HashMap.empty[Int, Int]
    for(i <- candidateItem.indices){
      for(j <- node.indices){
        if(itemsExistMap(j).contains(candidateItem(i))){
          var c = candidateItemAndCount.getOrElseUpdate(candidateItem(i), 0)
          c += count(j).count
          candidateItemAndCount.update(i, c)
        }
      }
    }
    val freqSingleItem = candidateItemAndCount.toList.filter(_._2 >= minCount).map(_._1)
    val freqItems = freqSingleItem.map(x => Array(x) ++ curItems).toArray
    (freqSingleItem, freqItems)
  }

  def getItemsAndCount(node: Array[Node], count: Array[Node], minCount: Int) = {
    val itemsAndCount = mutable.HashMap.empty[Int, Int]
    val itemsExistMap = mutable.HashMap.empty[Int, mutable.HashMap[Int, Boolean]]
    for(i <- node.indices){
      var curNode = node(i).parent
      itemsExistMap += i -> mutable.HashMap.empty[Int, Boolean]
      while(!curNode.isRoot){
        itemsExistMap(i) += curNode.item -> true
        var value = itemsAndCount.getOrElseUpdate(curNode.item, 0)
        value += count(i).count
        itemsAndCount.update(curNode.item, value)
        curNode = curNode.parent
      }
    }
    (itemsAndCount.toList.filter(_._2 >= minCount).toMap, itemsExistMap)
  }

}

object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node(val parent: Node) extends Serializable {
    var item: Int = _
    var count: Int = 0
    val children = mutable.Map.empty[Int, Node]

    def isRoot: Boolean = parent == null
  }

  /** Summary of an item in an FP-Tree. */
  private class Summary extends Serializable {
    var count: Int = 0
    val nodes = ListBuffer.empty[Node]
  }
}
