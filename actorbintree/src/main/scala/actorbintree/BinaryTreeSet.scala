/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, elem: Int) =>
      root ! Insert(requester, id, elem)
    case Contains(requester: ActorRef, id: Int, elem: Int) =>
      root ! Contains(requester, id, elem)
    case Remove(requester: ActorRef, id: Int, elem: Int) =>
      root ! Remove(requester, id, elem)
  }

  // optional

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = ???

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester: ActorRef, id: Int, value: Int) =>
      if (value == elem) {
        removed = false
        requester ! OperationFinished(id)
      } else if (value < elem) {
        insertIntoSubtree(Left, requester, id, value)
      } else {
        insertIntoSubtree(Right, requester, id, value)
      }
    case Contains(requester: ActorRef, id: Int, value: Int) =>
      if (value == elem) {
        requester ! ContainsResult(id, result = !removed)
      } else if (value < elem) {
        subtreeContains(Left, requester, id, value)
      } else {
        subtreeContains(Right, requester, id, value)
      }
    case Remove(requester: ActorRef, id: Int, value: Int) =>
      if (value == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else if (value < elem) {
        removeFromSubtree(Left, requester, id, value)
      } else if (value > elem) {
        removeFromSubtree(Right, requester, id, value)
      }
    case CopyTo => ???
  }

  def insertIntoSubtree(position: Position, requester: ActorRef, id: Int, value: Int): Unit = {
    subtrees.get(position) match {
      case Some(leaf) => leaf ! Insert(requester, id, value)
      case None =>
        val leaf = context.actorOf(props(value, initiallyRemoved = false))
        subtrees += (position -> leaf)
        requester ! OperationFinished(id)
    }
  }

  def subtreeContains(position: Position, requester: ActorRef, id: Int, value: Int): Unit = {
    subtrees.get(position) match {
      case Some(leaf) => leaf ! Contains(requester, id, value)
      case None => requester ! ContainsResult(id, result = false)
    }
  }

  def removeFromSubtree(position: Position, requester: ActorRef, id: Int, value: Int): Unit = {
    subtrees.get(position) match {
      case Some(leaf) => leaf ! Remove(requester, id, value)
      case None => requester ! OperationFinished(id)
    }
  }

  // optional

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = ???


}
