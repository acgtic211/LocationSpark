package cs.purdue.edu.spatialindex.rtree

import java.util

import scala.collection.mutable.HashSet
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Try

object RTree {

  /**
   * Construct an empty RTree.
   */
  def empty[V]: RTree[V] = new RTree(Node.empty[V], 0)

  /**
   * Construct an RTree from a sequence of entries.
   */
  def apply[V](entries: Entry[V]*): RTree[V] =
    entries.foldLeft(RTree.empty[V])(_ insert _)

  def apply[A](itr:Iterator[Entry[A]])=
  {
    itr.foldLeft(RTree.empty[A])(_ insert _)
  }
}

/**
 * This is the magnificent RTree, which makes searching ad-hoc
 * geographic data fast and fun.
 *
 * The RTree wraps a node called 'root' that is the actual root of the
 * tree structure. RTree also keeps track of the total size of the
 * tree (something that individual nodes don't do).
 */
case class RTree[V](root: Node[V], size: Int) {

  /**
   * Typesafe equality test.
   *
   * In order to be considered equal, two trees must have the same
   * number of entries, and each entry found in one should be found in
   * the other.
   */
  def ===(that: RTree[V]): Boolean =
    size == that.size && entries.forall(that.contains)

  /**
   * Universal equality test.
   *
   * Trees can only be equal to other trees. Unlike some other
   * containers, the trees must be parameterized on the same type, or
   * else the comparison will fail.
   *
   * This means comparing an RTree[Int] and an RTree[BigInt] will
   * always return false.
   */
  override def equals(that: Any): Boolean =
    that match {
      case rt: RTree[_] =>
        Try(this === rt.asInstanceOf[RTree[V]]).getOrElse(false)
      case _ =>
        false
    }

  /**
   * Universal hash code method.
   */
  override def hashCode(): Int = {
    var x = 0xbadd0995
    val it = entries
    while (it.hasNext) x ^= (it.next.hashCode * 777 + 1)
    x
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x: Float, y: Float, value: V): RTree[V] =
    insert(Entry(Point(x, y), value))


  /**
   * Insert an entry into the tree, returning a new tree.
   */
  def insert(entry: Entry[V]): RTree[V] = {
    val r = root.insert(entry) match {
      case Left(rs) => Branch(rs, rs.foldLeft(Box.empty)(_ expand _.box))
      case Right(r) => r
    }
    RTree(r, size + 1)
  }

  /**
   * Insert a value into the tree at (x, y), returning a new tree.
   */
  def insert(x:Point, value: V): RTree[V] =
    insert(Entry(x, value))


  /**
   * Insert entries into the tree, returning a new tree.
   */
  def insertAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ insert _)

  /**
   * Remove an entry from the tree, returning a new tree.
   *
   * If the entry was not present, the tree is simply returned.
   */
  def remove(entry: Entry[V]): RTree[V] =
    root.remove(entry) match {
      case None =>
        this
      case Some((es, None)) =>
        es.foldLeft(RTree.empty[V])(_ insert _)
      case Some((es, Some(node))) =>
        es.foldLeft(RTree(node, size - es.size - 1))(_ insert _)
    }

  /**
   * Remove entries from the tree, returning a new tree.
   */
  def removeAll(entries: Iterable[Entry[V]]): RTree[V] =
    entries.foldLeft(this)(_ remove _)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box): Seq[Entry[V]] =
    root.search(space, _ => true)

  /**
   * this is the dual tree join algorithm,
   * this one the data tree, and stree is the query tree.
   * @param stree
   * @return
   */
  def join(stree:RTree[V]):Seq[Entry[V]] =
  {
    val buf = HashSet.empty[Entry[V]]

    //this changed to generic search
    //recursive
    //case 3: the data tree is leaf, but the query tree is branch
    def recur(node: Node[V], entry:Entry[V]): Unit =
      node match {
      case Leaf(children, box) =>
        children.foreach { c =>
          if (c.geom.contains(entry.geom))
             buf.add(entry)
        }
      case Branch(children, box) =>
        children.foreach { c =>
          if (c.box.intersects(entry.geom)) recur(c,entry)
        }
    }

    //this changed to generic search
    //recursive to the leaf node, which contain the data itself
    //case 4: the data tree is branch, but the query tree is leaf
    def recur2(node: Node[V], querybox:Geom): Unit =
      node match {
        case Leaf(children, box) =>
          children.foreach { c =>
            if (querybox.contains(c.geom))
              buf.add(c)
          }
        case Branch(children, box) =>
          children.foreach { c =>
            if (querybox.intersects(c.geom)) recur2(c,querybox)
          }
      }

    //use the line swipe based approach to find the intersection of two rectangle sets
    def intersection(r:Vector[Node[V]],s:Vector[Node[V]]):Seq[(Node[V],Node[V])]=
    {
      val rsort=r.sortBy(e=>e.geom.x)
      val ssort=s.sortBy(e=>e.geom.x)

      val buf = ArrayBuffer.empty[(Node[V], Node[V])]

      def internalloop(entry:Node[V], marked:Int, s:Vector[Node[V]]): Unit =
      {
        var i=marked
        while(i<s.size&&s(i).geom.x<=entry.geom.x2)
        {
          if(entry.geom.y<=s(i).geom.y2&&entry.geom.y2>=s(i).geom.y)
          {
             buf.+=(entry->s(i))
          }
          i+=1
        }
      }

      var i=0
      var j=0

      while(i<rsort.size&&j<ssort.size)
      {
           if(rsort(i).geom.x<ssort(j).geom.x)
           {
             internalloop(rsort(i),j,ssort)
             i+=1
           }else
           {
             internalloop(ssort(j),i,ssort)
             j+=1
           }
      }
      buf
    }

    //use the line swipe based approach to find the intersection of two rectangle sets
    def intersectionForleaf(r:Vector[Entry[V]],s:Vector[Entry[V]]):Seq[(Entry[V],Entry[V])]=
    {
      val rsort=r.sortBy(e=>e.geom.x)
      val ssort=s.sortBy(e=>e.geom.x)

      val buf = ArrayBuffer.empty[(Entry[V], Entry[V])]

      def internalloop(entry:Entry[V], marked:Int, s:Vector[Entry[V]]): Unit =
      {
        var i=marked
        while(i<s.size&&s(i).geom.x<=entry.geom.x2)
        {
          if(entry.geom.y<=s(i).geom.y2&&entry.geom.y2>=s(i).geom.y)
          {
            buf.+=(entry->s(i))
          }
          i+=1
        }
      }

      var i=0
      var j=0

      while(i<rsort.size&&j<ssort.size)
      {
        if(rsort(i).geom.x<ssort(j).geom.x)
        {
          internalloop(rsort(i),j,ssort)
          i+=1
        }else
        {
          internalloop(ssort(j),i,ssort)
          j+=1
        }
      }
      buf
    }

    //this is used for spatial join
    def sjoin(rnode:Node[V],snode:Node[V]):Unit={

      if(rnode.box.intersects(snode.box))
      {
         val intesectionbox=rnode.box.intesectBox(snode.box)

        rnode match {

          case Leaf(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 2: both tree reach the leaf node

                children_r.foreach{
                  cr=>
                    children_s.foreach {
                      cs =>
                        if (cs.geom.contains(cr.geom))
                          buf.add(cr) //return the results
                    }
                }

                /*intersectionForleaf(children_s,children_s).foreach {
                  case (cr, cs) =>
                    if(cs.geom.contains(cr.geom))
                    buf.append(cr) //return the results
                }*/


              case Branch(children_s, box_s)=> //case 3: the data tree is leaf, but the query tree is branch
              {
                val r=children_r.filter(entry=>entry.geom.intersects(intesectionbox))
                val s=children_s.filter(node=>node.box.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur(cs,cr)
                    }
                }

              }
            }

          case Branch(children_r, box_r) =>

            snode match
            {
              case Leaf(children_s, box_s) => //case 4: the data tree is branch, but the query tree is leaf

                val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                val s=children_s.filter(entry=>entry.geom.intersects(intesectionbox))

                r.foreach{
                  cr=>
                    s.foreach {
                      cs =>
                        recur2(cr,cs.geom)
                    }
                }

              case Branch(children_s, box_s)=> //case 1: both of two tree are branch
              {
                  val r=children_r.filter(node=>node.box.intersects(intesectionbox))
                  val s=children_s.filter(node=>node.box.intersects(intesectionbox))
                //nest loop to recursive on those dataset

                //use the sorted based approach for the intersection aprt
                //by this way, the nest-loop is changed to o(n)+k

                /*intersection(r,s).foreach
                {
                  case(rc,sc)=>
                    sjoin(rc,sc)
                }*/

                  r.foreach {
                    rc =>
                    s.foreach{
                      sc=>
                        if(rc.box.intersects(sc.box))
                          sjoin(rc,sc)
                    }
                  }

                //next step to test on sort and plan sweep approach
              }
            }
        }
      }

    }

    sjoin(this.root,stree.root)

    buf.toSeq
  }


  def cleanTree()=
  {
    def recur(node:Node[V]):Unit= {

      node match {
        case Leaf(children, box) =>
          children.drop(children.size - 1)
        case Branch(children, box) =>

          children.foreach { c =>
            recur(c)
          }
          children.drop(children.size - 1)
      }
    }

      recur(this.root)
  }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def searchPoint(pt: Point): Entry[V] =
  {
    val ret=root.search(pt.toBox, _ => true)

    if(ret!=null&&ret.length!=0)
      ret.head
    else
      null

  }


  /**
   * Return a sequence of all entries found in the given search space.
   */
  def search(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.search(space, f)



  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box): Seq[Entry[V]] =
    root.searchIntersection(space, _ => true)

  /**
   * Return a sequence of all entries intersecting the given search space.
   */
  def searchIntersection(space: Box, f: Entry[V] => Boolean): Seq[Entry[V]] =
    root.searchIntersection(space, f)

  /**
   * Construct a result an initial value, the entries found in a
   * search space, and a binary function `f`.
   *
   *   rtree.foldSearch(space, init)(f)
   *
   * is equivalent to (but more efficient than):
   *
   *   rtree.search(space).foldLeft(init)(f)
   */
  def foldSearch[B](space: Box, init: B)(f: (B, Entry[V]) => B): B =
    root.foldSearch(space, init)(f)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearest(pt: Point): Option[Entry[V]] =
    root.nearest(pt, Float.PositiveInfinity).map(_._2)

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point, k: Int): IndexedSeq[Entry[V]] =
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      root.nearestK(pt, k, Double.PositiveInfinity, pq)
      val arr = new Array[Entry[V]](pq.size)
      var i = arr.length - 1
      while (i >= 0) {
        val (_, e) = pq.dequeue
        arr(i) = e
        i -= 1
      }
      arr
    }

  /**
   * Return a sequence of all entries found in the given search space.
   */
  def nearestK(pt: Point,k: Int,z:Entry[V]=>Boolean): IndexedSeq[(Double,Entry[V])] = {
    if (k < 1) {
      Vector.empty
    } else {
      implicit val ord = Ordering.by[(Double, Entry[V]), Double](_._1)
      val pq = PriorityQueue.empty[(Double, Entry[V])]
      root.nearestK(pt, k, Double.PositiveInfinity, z, pq)
      pq.toIndexedSeq
    }
  }

  /**
   * Return a count of all entries found in the given search space.
   */
  def count(space: Box): Int =
    root.count(space)

  /**
   * Return whether or not the value exists in the tree at (x, y).
   */
  def contains(x: Float, y: Float, value: V): Boolean =
    root.contains(Entry(Point(x, y), value))

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def contains(entry: Entry[V]): Boolean =
    root.contains(entry)

  /**
   * Return whether or not the given entry exists in the tree.
   */
  def contains[K](k:K, value:V): Boolean =
  {
    root.contains(Entry(k.asInstanceOf[Point], value) )
  }


  /**
   * Map the entry values from A to B.
   */
  def map[B](f: V => B): RTree[B] =
    RTree(root.map(f), size)

  /**
   * Return an iterator over all entries in the tree.
   */
  def entries: Iterator[Entry[V]] =
    root.iterator

  /**
   * Return an iterator over all values in the tree.
   */
  def values: Iterator[V] =
    entries.map(_.value)

  /**
   * Return a nice depiction of the tree.
   *
   * This method should only be called on small-ish trees! It will
   * print one line for every branch, leaf, and entry, so for a tree
   * with thousands of entries this will result in a very large
   * string!
   */
  def pretty: String = root.pretty

  override def toString: String =
    s"RTree(<$size entries>)"
}
