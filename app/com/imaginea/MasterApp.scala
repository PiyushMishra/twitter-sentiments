package com.imaginea

import java.net.InetAddress

import akka.actor._
import akka.routing.RoundRobinGroup
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.Future
import akka.pattern.ask
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object MasterApp {
implicit val timeout = Timeout(5 minutes)

  def configString = s"""akka {
                        |  actor {
                        |    provider = "akka.remote.RemoteActorRefProvider"
                        |  }
                        |  remote {
                        |    enabled-transports = ["akka.remote.netty.tcp"]
                        |    netty.tcp {
                        |      hostname = ${InetAddress.getLocalHost.getHostAddress()}
                        |      port = 0
                        |    }
                        |  }
                        |}""".stripMargin

  val config = ConfigFactory.parseString(configString)
  val actorSystem = ActorSystem("twitter", config)
  val querySearchConfig = ConfigFactory.load("querySearch.conf")
  val ips = querySearchConfig.getString("remoteIps").split(",")
  val paths = ips.map(getRemoteActorPath(_)).toList
  val remoteRouter: ActorRef =
    actorSystem.actorOf(RoundRobinGroup(paths).props())
  def process(terms: List[String], days: String) =  {
//    val aggregator: ActorRef = actorSystem.actorOf(Props(classOf[Aggregator], remoteRouter))
    val future = terms.map(term => (remoteRouter ? QueryTwitter(term, days)).mapTo[TermWithCount])
    Future.sequence(future)
  }

  def getRemoteActorPath(ip: String) = s"akka.tcp://twitter@$ip:2552/user/router"
}

class Aggregator(remoteRouter: ActorRef) extends Actor {
  var sendCount = 0
  var receiveCount = 0
  var termWithCountList = List[TermWithCount]()
  def receive: Receive = {
    case (term: String, days: String) => remoteRouter ! QueryTwitter(term, days)
      sendCount = sendCount + 1
    case TermWithCount(term, tweetCount) => receiveCount = receiveCount + 1
      termWithCountList = termWithCountList ++ List(TermWithCount(term, tweetCount))
      if (receiveCount == sendCount) {
        JsonUtils .toJson(TermWithCounts(termWithCountList))
      }
  }
}