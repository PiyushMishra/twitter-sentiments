package com.imaginea

import java.net.InetAddress

import akka.actor._
import akka.routing.RoundRobinGroup
import com.typesafe.config.ConfigFactory

object MasterApp {

  def configString = s"""akka {
                        |  actor {
                        |    provider = "akka.remote.RemoteActorRefProvider"
                        |  }
                        |  remote {
                        |    enabled-transports = ["akka.remote.netty.tcp"]
                        |    netty.tcp {
                        |      hostname = ${InetAddress.getLocalHost.getHostAddress()}
                        |      port = 2554
                        |    }
                        |  }
                        |}""".stripMargin

  val config = ConfigFactory.parseString(configString)
  val actorSystem = ActorSystem("twitter", config)

  def process(terms: List[String], days: String) {
    val config = ConfigFactory.load("querySearch.conf")
    val ips = config.getString("remoteIps").split(",")
    val paths = ips.map(getRemoteActorPath(_)).toList
    val remoteRouter: ActorRef =
      actorSystem.actorOf(RoundRobinGroup(paths).props())
    val aggregator: ActorRef = actorSystem.actorOf(Props(classOf[Aggregator], remoteRouter))
    terms.foreach(term => aggregator ! (term, days))
  }
  def getRemoteActorPath(ip: String) = s"akka.tcp://twitter@$ip:2552/user/router"
}

class Aggregator(remoteRouter: ActorRef) extends Actor {
  var sendCount = 0
  var receiveCount = 0
  val termWithCountMap = scala.collection.mutable.Map[String, Int]()

  def receive: Receive = {
    case (term: String, days: String)=> remoteRouter ! QueryTwitter(term, days, self)
      sendCount = sendCount + 1
    case TermWithCount(term, tweetCount) => receiveCount = receiveCount + 1
      termWithCountMap.put(term, tweetCount)
      if (receiveCount == sendCount) {
        QuerySearch.toJson(Terms(termWithCountMap))
        println("done")
        self ! PoisonPill
        remoteRouter ! PoisonPill
        /* Launcher.runSparkJob("/opt/software/spark-1.3.1-bin-hadoop2.6/",
           "/opt/sentiment-assembly-1.0.jar",
           "com.imaginea.SentimentAnalysis", "spark://HadoopMaster:7077")*/
      }
  }
}

case class Terms(analyzedTerms: scala.collection.mutable.Map[String, Int])
