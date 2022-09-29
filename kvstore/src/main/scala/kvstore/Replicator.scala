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
  var acknowledgements = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  context.system.scheduler.scheduleAtFixedRate(100.milliseconds, 100.milliseconds) { () =>
    acknowledgements.toSeq.sortBy(_._1) foreach {
      case (seq, (_, message)) => replica ! Snapshot(message.key, message.valueOption, seq)
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
    case message: Replicate =>
      val seq = nextSeq()
      acknowledgements += (seq -> (sender, message))
      replica ! Snapshot(message.key, message.valueOption, seq)
    case SnapshotAck(key, seq) =>
      acknowledgements.get(seq) foreach {
        case (client, request) =>
          acknowledgements -= seq
          client ! Replicated(key, request.id)
      }
  }

}
