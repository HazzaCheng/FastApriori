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
