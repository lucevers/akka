/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.contrib.pattern

import language.postfixOps
import scala.collection.immutable
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.Member
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.remote.testkit.STMultiNodeSpec
import akka.testkit._
import akka.testkit.ImplicitSender
import akka.testkit.TestEvent._

object ClusterSingletonSpec extends MultiNodeConfig {
  val controller = role("controller")
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")
  val fifth = role("fifth")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
    akka.remote.log-remote-lifecycle-events = off
    akka.cluster.auto-join = off
    akka.cluster.auto-down = on
    """))

  testTransport(on = true)

  object PointToPointChannel {
    case object RegisterConsumer
    case object UnregisterConsumer
    case object RegistrationOk
    case object UnexpectedRegistration
    case object UnregistrationOk
    case object UnexpectedUnregistration
    case object Reset
    case object ResetOk
  }

  class PointToPointChannel extends Actor with ActorLogging {
    import PointToPointChannel._

    def receive = idle

    def idle: Receive = {
      case RegisterConsumer ⇒
        log.info("RegisterConsumer: [{}]", sender.path)
        sender ! RegistrationOk
        context.become(active(sender))
      case UnregisterConsumer ⇒
        log.info("UnexpectedUnregistration: [{}]", sender.path)
        sender ! UnexpectedUnregistration
      case Reset ⇒ sender ! ResetOk
      case msg   ⇒ // no consumer, drop
    }

    def active(consumer: ActorRef): Receive = {
      case UnregisterConsumer if sender == consumer ⇒
        log.info("UnregistrationOk: [{}]", sender.path)
        sender ! UnregistrationOk
        context.become(idle)
      case UnregisterConsumer ⇒
        log.info("UnexpectedUnregistration: [{}], expected [{}]", sender.path, consumer.path)
        sender ! UnexpectedUnregistration
      case RegisterConsumer ⇒
        log.info("Unexpected RegisterConsumer [{}], active consumer [{}]", sender.path, consumer.path)
        sender ! UnexpectedRegistration
      case Reset ⇒
        context.become(idle)
        sender ! ResetOk
      case msg ⇒ consumer ! msg
    }
  }

  object Consumer {
    case object End
    case object GetCurrent
  }

  /**
   * Singleton
   */
  class Consumer(handOverData: Option[Any], queue: ActorRef, delegateTo: ActorRef) extends Actor {
    import Consumer._
    import PointToPointChannel._

    var current: Int = handOverData match {
      case Some(x: Int) ⇒ x
      case Some(x)      ⇒ throw new IllegalArgumentException(s"handOverData must be an Int, got [${x}]")
      case None         ⇒ 0
    }

    override def preStart(): Unit = queue ! RegisterConsumer

    def receive = {
      case n: Int if n <= current ⇒
        context.stop(self)
      case n: Int ⇒
        current = n
        delegateTo ! n
      case x @ (RegistrationOk | UnexpectedRegistration) ⇒
        delegateTo ! x
      case GetCurrent ⇒
        sender ! current
      case End ⇒
        queue ! UnregisterConsumer
      case UnregistrationOk ⇒
        // reply to ClusterSingleton with hand over data,
        // which will be passed as parameter to new leader consumer
        context.parent ! current
        context stop self
    }
  }

}

class ClusterSingletonMultiJvmNode1 extends ClusterSingletonSpec
class ClusterSingletonMultiJvmNode2 extends ClusterSingletonSpec
class ClusterSingletonMultiJvmNode3 extends ClusterSingletonSpec
class ClusterSingletonMultiJvmNode4 extends ClusterSingletonSpec
class ClusterSingletonMultiJvmNode5 extends ClusterSingletonSpec
class ClusterSingletonMultiJvmNode6 extends ClusterSingletonSpec

class ClusterSingletonSpec extends MultiNodeSpec(ClusterSingletonSpec) with STMultiNodeSpec with ImplicitSender {
  import ClusterSingletonSpec._
  import ClusterSingletonSpec.PointToPointChannel._
  import ClusterSingletonSpec.Consumer._

  override def initialParticipants = roles.size

  // Sort the roles in the order used by the cluster.
  lazy val sortedClusterRoles: immutable.IndexedSeq[RoleName] = {
    implicit val clusterOrdering: Ordering[RoleName] = new Ordering[RoleName] {
      import Member.addressOrdering
      def compare(x: RoleName, y: RoleName) = addressOrdering.compare(node(x).address, node(y).address)
    }
    roles.filterNot(_ == controller).toVector.sorted
  }

  def queue: ActorRef = system.actorFor(node(controller) / "user" / "queue")

  def consumerProps(handOverData: Option[Any]) = Props(new Consumer(handOverData, queue, testActor))

  def createSingleton(): ActorRef =
    system.actorOf(Props(new ClusterSingleton(consumerProps, "consumer", End)), "singleton")

  def consumer(leader: RoleName): ActorRef = system.actorFor(node(leader) / "user" / "singleton" / "consumer")

  def verify(leader: RoleName, msg: Int, expectedCurrent: Int): Unit = {
    runOn(leader) {
      expectMsg(RegistrationOk)
      consumer(leader) ! GetCurrent
      expectMsg(expectedCurrent)
    }
    enterBarrier(leader.name + "-active")

    runOn(controller) {
      queue ! msg
    }
    runOn(leader) {
      expectMsg(msg)
    }
    runOn(sortedClusterRoles.filterNot(_ == leader): _*) {
      expectNoMsg(1 second)
    }
    enterBarrier(leader.name + "-verified")
  }

  "A ClusterSingleton" must {

    "startup in single member cluster" in {
      log.info("Sorted cluster nodes [{}]", sortedClusterRoles.map(node(_).address).mkString(", "))

      runOn(controller) {
        system.actorOf(Props[PointToPointChannel], "queue")
      }
      enterBarrier("queue-started")

      runOn(sortedClusterRoles.last) {
        Cluster(system) join node(sortedClusterRoles.last).address
        createSingleton()
      }

      verify(sortedClusterRoles.last, 1, 0)
    }

    "move when new leader" in {
      // last node is already used
      // first node will be used later
      // use nodes in between
      val current = Iterator from 1
      (sortedClusterRoles.size - 2) to 1 by -1 foreach { i ⇒
        within(10 seconds) {
          runOn(sortedClusterRoles(i)) {
            Cluster(system) join node(sortedClusterRoles.last).address
            createSingleton()
          }

          val expectedCurrent = current.next
          verify(sortedClusterRoles(i), expectedCurrent + 1, expectedCurrent)
        }
      }
    }

    "move when leader leaves" in within(15 seconds) {
      val leaveRole = sortedClusterRoles(1)
      val newLeaderRole = sortedClusterRoles(2)

      runOn(leaveRole) {
        Cluster(system) leave node(leaveRole).address
      }

      verify(newLeaderRole, 5, 4)
    }

    "move when leader crash" in within(25 seconds) {
      system.eventStream.publish(Mute(EventFilter.warning(pattern = ".*received dead letter from.*")))
      system.eventStream.publish(Mute(EventFilter.error(pattern = ".*Disassociated.*")))
      system.eventStream.publish(Mute(EventFilter.error(pattern = ".*Association failed.*")))

      val crashRole = sortedClusterRoles(2)
      val newLeaderRole = sortedClusterRoles(3)
      runOn(controller) {
        queue ! Reset
        expectMsg(ResetOk)
        log.info("Shutdown [{}]", node(crashRole).address)
        testConductor.shutdown(crashRole, 0).await
      }

      verify(newLeaderRole, 6, 0)
    }

    // FIXME test only one node remaining, I think something is missing for that

    // FIXME test some concurrent join and down

  }
}
