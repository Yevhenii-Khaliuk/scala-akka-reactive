package kvstore

import akka.actor.{Actor, ActorRef, Props}


object Replica {
  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Arbiter._
  import Replica._
  import Replicator._


  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistence: ActorRef = context.actorOf(persistenceProps)

  var expectedSeq = 0L

  var idCounter = 0L

  def nextId(): Long = {
    val ret = idCounter
    idCounter += 1
    ret
  }

  def receive: Receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      sender ! OperationAck(id)
    case Remove(key, id) =>
      kv -= key
      sender ! OperationAck(id)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      replicas - self diff secondaries.keySet foreach (secondary => {
        val replicator = context.actorOf(Replicator.props(secondary))
        replicators += replicator
        secondaries += secondary -> replicator
        kv foreach { case (key, value) => replicator ! Replicate(key, Some(value), nextId()) }
      })
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      seq match {
        case receivedSeq if receivedSeq > expectedSeq => ()
        case receivedSeq if receivedSeq < expectedSeq =>
          expectedSeq = receivedSeq + 1
          sender ! SnapshotAck(key, seq)
        case _ => valueOption match {
          case Some(value) =>
            kv += (key -> value)
            expectedSeq += 1
            sender ! SnapshotAck(key, seq)
          case None =>
            kv -= key
            expectedSeq += 1
            sender ! SnapshotAck(key, seq)
        }
      }
  }

  override def preStart(): Unit =
    arbiter ! Join
}

