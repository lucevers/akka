/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.pattern

import scala.concurrent.duration._
import scala.collection.immutable.Queue
import akka.actor.Actor
import akka.actor.Actor.Receive
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.FSM
import akka.actor.Props
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.AkkaException

object ClusterSingleton {

  // FIXME those must be public due to the `with FSM` type parameters, would like to place them in Internal
  sealed trait State
  sealed trait Data

  private object Internal {
    case object TakeOverFromMe
    case object HandOverToMe
    case object HandOverInProgress
    case class HandOverDone(handOverData: Option[Any])
    case class Retry(count: Int)
    case class TakeOverRetry(leaderPeer: ActorRef, count: Int)

    case object Start extends State
    case object Leader extends State
    case object NonLeader extends State
    case object BecomingLeader extends State
    case object WasLeader extends State
    case object HandingOver extends State
    case object TakeOver extends State

    case object Uninitialized extends Data
    case class NonLeaderData(leader: Option[Address], leaderDown: Boolean = false) extends Data
    case class BecomingLeaderData(previousLeader: Option[Address]) extends Data
    case class LeaderData(child: ActorRef) extends Data
    case class WasLeaderData(child: ActorRef, newLeader: Address) extends Data
    case class HandingOverData(child: ActorRef, handOverTo: Option[ActorRef], handOverData: Option[Any]) extends Data

    val HandOverRetryTimer = "hand-over-retry"
    val TakeOverRetryTimer = "take-over-retry"

    class SingletonException(message: String) extends AkkaException(message, null)

    object LeaderChangedBuffer {
      case object GetNext
      case class InitialLeaderState(leader: Option[Address], memberCount: Int)
    }

    class LeaderChangedBuffer extends Actor {
      import LeaderChangedBuffer._

      val cluster = Cluster(context.system)
      var changes = Queue.empty[AnyRef]
      var memberCount = 0

      // subscribe to cluster changes, LeaderChanged
      // re-subscribe when restart
      override def preStart(): Unit = cluster.subscribe(self, classOf[LeaderChanged])
      override def postStop(): Unit = cluster.unsubscribe(self)

      def receive = {
        case state: CurrentClusterState ⇒
          changes = changes enqueue InitialLeaderState(state.leader, state.members.size)
        case event: LeaderChanged ⇒
          changes = changes enqueue event
        case GetNext if changes.isEmpty ⇒
          context.become(deliverNext, discardOld = false)
        case GetNext ⇒
          val (event, remaining) = changes.dequeue
          changes = remaining
          context.parent ! event
      }

      def deliverNext: Actor.Receive = {
        case state: CurrentClusterState ⇒
          context.parent ! InitialLeaderState(state.leader, state.members.size)
          context.unbecome()
        case event: LeaderChanged ⇒
          context.parent ! event
          context.unbecome()
      }

    }

  }
}

class ClusterSingleton(
  childProps: Option[Any] ⇒ Props,
  childName: String,
  terminationMessage: Any,
  maxRetries: Int = 20,
  retryInterval: FiniteDuration = 1.second)
  extends Actor with FSM[ClusterSingleton.State, ClusterSingleton.Data] {

  import ClusterSingleton._
  import ClusterSingleton.Internal._
  import ClusterSingleton.Internal.LeaderChangedBuffer._

  val cluster = Cluster(context.system)
  val selfAddressOption = Some(cluster.selfAddress)
  var selfRemoved = false

  val leaderChangedBuffer = context.actorOf(Props[LeaderChangedBuffer])

  // subscribe to cluster changes
  // re-subscribe when restart
  override def preStart(): Unit = {
    if (cluster.isTerminated)
      throw new SingletonException("Cluster node is terminated")

    cluster.subscribe(self, classOf[MemberDowned])
    cluster.subscribe(self, classOf[MemberRemoved])
    leaderChangedBuffer ! GetNext
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def peer(at: Address): ActorRef = context.actorFor(self.path.toStringWithAddress(at))

  startWith(Start, Uninitialized)

  when(Start) {
    case Event(InitialLeaderState(leader, memberCount), _) ⇒
      if (leader == selfAddressOption && memberCount == 1)
        // alone, leader immediately
        gotoLeader(None)
      else if (leader == selfAddressOption)
        goto(BecomingLeader) using BecomingLeaderData(previousLeader = None)
      else
        goto(NonLeader) using NonLeaderData(leader)
  }

  when(NonLeader) {
    case Event(LeaderChanged(leader), NonLeaderData(previousLeader, leaderDown)) ⇒
      log.info("NonLeader observed LeaderChanged: [{} -> {}]", previousLeader, leader)
      if (leader == selfAddressOption) {
        if (leaderDown)
          gotoLeader(None)
        else {
          previousLeader foreach { peer(_) ! HandOverToMe }
          goto(BecomingLeader) using BecomingLeaderData(previousLeader)
        }
      } else {
        leaderChangedBuffer ! GetNext
        stay
      }

    case Event(MemberDowned(m), NonLeaderData(Some(previousLeader), false)) if m.address == previousLeader ⇒
      log.info("Previous leader downed [{}]", m.address)
      // transition when LeaderChanged
      stay using NonLeaderData(leader = None, leaderDown = true)

    case Event(MemberRemoved(m), _) if m.address == cluster.selfAddress ⇒
      log.info("Self removed, stopping singleton")
      selfRemoved = true
      stop()

    case Event(TakeOverFromMe, NonLeaderData(None, _)) ⇒
      log.info("Take over request from [{}]", sender.path.address)
      // transition when LeaderChanged
      stay using NonLeaderData(Some(sender.path.address))

    case Event(TakeOverFromMe, NonLeaderData(Some(_), _)) ⇒
      // we already know who was the previousLeader, transition when LeaderChanged
      stay
  }

  when(BecomingLeader) {

    case Event(HandOverInProgress, _) ⇒
      // confirmation that the hand over process has started
      cancelTimer(HandOverRetryTimer)
      stay

    case Event(HandOverDone(handOverData), BecomingLeaderData(Some(previousLeader))) if sender.path.address == previousLeader ⇒
      gotoLeader(handOverData)

    case Event(MemberDowned(m), BecomingLeaderData(Some(previousLeader))) if m.address == previousLeader ⇒
      log.info("Previous leader [{}] downed", previousLeader)
      gotoLeader(None)

    case Event(TakeOverFromMe, BecomingLeaderData(None)) ⇒
      sender ! HandOverToMe
      stay using BecomingLeaderData(Some(sender.path.address))

    case Event(TakeOverFromMe, BecomingLeaderData(Some(previousLeader))) ⇒
      if (previousLeader == sender.path.address) {
        sender ! HandOverToMe
        stay
      } else {
        log.info("Ignoring TakeOver request from [{}]. Expected previous leader [{}]", sender.path.address, previousLeader)
        stay
      }

    case Event(Retry(count), BecomingLeaderData(previousLeader)) ⇒
      previousLeader foreach { a ⇒
        log.info("Retry [{}], sending HandOverToMe to [{}]", count, previousLeader)
        peer(a) ! HandOverToMe
      }
      if (count <= maxRetries)
        setTimer(HandOverRetryTimer, Retry(count + 1), retryInterval, repeat = false)
      else
        throw new SingletonException(s"Becoming singleton leader failed because previous leader [${previousLeader}] is unresponsive")
      stay

  }

  def gotoLeader(handOverData: Option[Any]): State = {
    log.info("Singleton leader [{}] starting child", cluster.selfAddress)
    val child = context watch context.actorOf(childProps(handOverData), childName)
    goto(Leader) using LeaderData(child)
  }

  onTransition {
    case _ -> BecomingLeader ⇒ setTimer(HandOverRetryTimer, Retry(1), retryInterval, repeat = false)
    case BecomingLeader -> _ ⇒ cancelTimer(HandOverRetryTimer)
  }

  when(Leader) {
    case Event(LeaderChanged(leader), LeaderData(child)) ⇒
      log.info("Leader observed LeaderChanged: [{} -> {}]", cluster.selfAddress, leader)
      leader match {
        case Some(a) if a != cluster.selfAddress ⇒
          // send take over request in case the new leader doesn't know previous leader
          val leaderPeer = peer(a)
          leaderPeer ! TakeOverFromMe
          setTimer(TakeOverRetryTimer, TakeOverRetry(leaderPeer, 1), retryInterval, repeat = false)
          goto(WasLeader) using WasLeaderData(child, newLeader = a)
        case _ ⇒
          // new leader will initiate the hand over
          stay
      }

    case Event(HandOverToMe, LeaderData(child)) ⇒
      gotoHandingOver(child, Some(sender))
  }

  onTransition {
    case WasLeader -> _ ⇒ cancelTimer(TakeOverRetryTimer)
  }

  when(WasLeader) {
    case Event(TakeOverRetry(leaderPeer, count), _) ⇒
      val newLeader = leaderPeer.path.address
      log.info("Retry [{}], sending TakeOverFromMe to [{}]", count, newLeader)
      leaderPeer ! TakeOverFromMe
      if (count <= maxRetries)
        setTimer(TakeOverRetryTimer, TakeOverRetry(leaderPeer, count + 1), retryInterval, repeat = false)
      else
        throw new SingletonException(s"Expected hand over to [${newLeader}] never occured")
      stay

    case Event(HandOverToMe, WasLeaderData(child, _)) ⇒
      gotoHandingOver(child, Some(sender))

    case Event(MemberDowned(m), WasLeaderData(child, newLeader)) if m.address == newLeader ⇒
      gotoHandingOver(child, None)

  }

  def gotoHandingOver(child: ActorRef, handOverTo: Option[ActorRef]): State = {
    handOverTo foreach { _ ! HandOverInProgress }
    child ! terminationMessage
    goto(HandingOver) using HandingOverData(child, handOverTo, None)
  }

  when(HandingOver) {
    case (Event(Terminated(ref), HandingOverData(child, handOverTo, handOverData))) if ref == child ⇒
      val newLeader = handOverTo.map(_.path.address)
      log.info("Singleton child terminated, hand over done [{} -> {}]", cluster.selfAddress, newLeader)
      handOverTo foreach { _ ! HandOverDone(handOverData) }
      goto(NonLeader) using NonLeaderData(newLeader)

    case Event(HandOverToMe, d @ HandingOverData(child, handOverTo, _)) if handOverTo == Some(sender) ⇒
      // retry
      sender ! HandOverInProgress
      stay

    case Event(childHandOverMessage, d @ HandingOverData(child, _, None)) if sender == child ⇒
      stay using d.copy(handOverData = Some(childHandOverMessage))

  }

  whenUnhandled {
    case Event(_: CurrentClusterState, _) ⇒ stay
    case Event(MemberRemoved(m), _) if m.address == cluster.selfAddress ⇒
      // will be stopped onTranstion to NonLeader
      selfRemoved = true
      stay
    case Event(MemberRemoved(m), _) ⇒
      log.info("Member removed [{}]", m.address)
      stay
    case Event(MemberDowned(m), _) ⇒
      log.info("Member downed [{}]", m.address)
      stay
  }

  onTransition {
    case _ -> (NonLeader | Leader) ⇒ leaderChangedBuffer ! GetNext
  }

  onTransition {
    case _ -> NonLeader if selfRemoved ⇒
      log.info("Self removed, stopping singleton")
      stop()
  }

  onTransition {
    case from -> to ⇒ log.info("Singleton state change [{} -> {}]", from, to)
  }

}