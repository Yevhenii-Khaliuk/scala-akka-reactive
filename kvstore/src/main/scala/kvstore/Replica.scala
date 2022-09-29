package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, ReceiveTimeout, SupervisorStrategy}

import scala.concurrent.duration.{Duration, DurationInt}


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

  case class Acknowledgement(client: ActorRef, key: String, value: Option[String],
                             persisted: Boolean = false, replicated: Int = 0)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Arbiter._
  import Persistence._
  import Replica._
  import Replicator._
  import context.dispatcher

  override val supervisorStrategy: SupervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => SupervisorStrategy.Resume
  }

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var persistence: ActorRef = context.actorOf(persistenceProps)

  var acknowledgements = Map.empty[Long, Acknowledgement]

  context.system.scheduler.scheduleAtFixedRate(100.milliseconds, 100.milliseconds) { () =>
    acknowledgements.toSeq.sortBy(_._1) foreach {
      case (id, ack) =>
        if (!ack.persisted)
          persistence ! Persist(ack.key, ack.value, id)
    }
  }

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

  val leader: Receive = {
    case Insert(key, value, id) =>
      kv += (key -> value)
      acknowledgements += id -> Acknowledgement(sender, key, Some(value))
      persistence ! Persist(key, Some(value), id)
      replicators foreach (_ ! Replicate(key, Some(value), id))
      context.setReceiveTimeout(1.second)

    case Remove(key, id) =>
      kv -= key
      acknowledgements += id -> Acknowledgement(sender, key, None)
      persistence ! Persist(key, None, id)
      replicators foreach (_ ! Replicate(key, None, id))
      context.setReceiveTimeout(1.second)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Persisted(_, id) =>
      val ack = acknowledgements(id)
      if (ack.replicated == replicators.size) {
        acknowledgements -= id
        context.setReceiveTimeout(Duration.Undefined)
        ack.client ! OperationAck(id)
      } else {
        acknowledgements +=
          id -> Acknowledgement(ack.client, ack.key, ack.value, persisted = true, ack.replicated)
      }

    case Replicated(_, id) =>
      acknowledgements.get(id) foreach { ack =>
        acknowledgeReplication(id, ack)
      }

    case Replicas(replicas) =>
      secondaries.keySet diff replicas - self foreach { removedReplica =>
        acknowledgements foreach { case (id, ack) =>
          acknowledgeReplication(id, ack)
        }
        val replicatorToRemove = secondaries(removedReplica)
        context.stop(replicatorToRemove)
        replicators -= replicatorToRemove
        secondaries -= removedReplica
      }

      replicas - self diff secondaries.keySet foreach (secondary => {
        val replicator = context.actorOf(Replicator.props(secondary))
        replicators += replicator
        secondaries += secondary -> replicator
        kv foreach { case (key, value) => replicator ! Replicate(key, Some(value), nextId()) }
      })

    case ReceiveTimeout =>
      if (acknowledgements.nonEmpty) {
        val id = acknowledgements.keySet.min
        val client = acknowledgements(id).client
        acknowledgements -= id
        client ! OperationFailed(id)
      }
  }

  private def acknowledgeReplication(id: Long, ack: Acknowledgement): Unit = {
    if (ack.persisted && ack.replicated + 1 == replicators.size) {
      acknowledgements -= id
      context.setReceiveTimeout(Duration.Undefined)
      ack.client ! OperationAck(id)
    } else {
      acknowledgements +=
        id -> Acknowledgement(ack.client, ack.key, ack.value, replicated = ack.replicated + 1)
    }
  }

  val replica: Receive = {
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case message: Snapshot =>
      message.seq match {
        case receivedSeq if receivedSeq > expectedSeq => ()
        case receivedSeq if receivedSeq < expectedSeq =>
          sender ! SnapshotAck(message.key, receivedSeq)
        case _ => message.valueOption match {
          case Some(value) =>
            kv += (message.key -> value)
            persistIntoReplicaStorage(message)
          case None =>
            kv -= message.key
            persistIntoReplicaStorage(message)
        }
      }

    case Persisted(key, id) =>
      val ack = acknowledgements(id)
      acknowledgements -= id
      ack.client ! SnapshotAck(key, id)
  }

  private def persistIntoReplicaStorage(message: Snapshot): Unit = {
    expectedSeq = message.seq + 1
    acknowledgements += message.seq -> Acknowledgement(sender, message.key, message.valueOption)
    persistence ! Persist(message.key, message.valueOption, message.seq)
  }

  override def preStart(): Unit =
    arbiter ! Join
}

