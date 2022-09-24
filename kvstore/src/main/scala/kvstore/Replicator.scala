package kvstore

import akka.actor.{Actor, ActorRef, Props}

import scala.concurrent.duration.DurationInt

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._
  import context.dispatcher


  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  context.system.scheduler.scheduleAtFixedRate(100.milliseconds, 100.milliseconds) { () =>
    acks.toSeq.sortBy(_._1) foreach {
      case (seq, (_, request)) => replica ! Snapshot(request.key, request.valueOption, seq)
    }
  }

  var _seqCounter = 0L

  def nextSeq(): Long = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }


  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      val seq = nextSeq()
      acks += seq -> (sender, Replicate(key, valueOption, id))
      replica ! Snapshot(key, valueOption, seq)
    case SnapshotAck(key, seq) =>
      val (leader, request) = acks(seq)
      acks -= seq
      leader ! Replicated(key, request.id)
  }

}
