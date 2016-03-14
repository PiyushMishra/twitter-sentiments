package com.imaginea

import java.net.InetAddress
import java.util.Date

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import com.fasterxml.jackson.annotation.JsonValue
import com.typesafe.config.ConfigFactory
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentType
import twitter4j._

import scala.collection.JavaConversions._

case class TweetsSentiment(tweet: Status, sentiment: Double)

trait TwitterInstance {
  val twitter = new TwitterFactory(ConfigurationBuilderUtil.buildConfiguration).getInstance()
}


object QuerySearch extends TwitterInstance {
  def configString = s"""akka {
                        |  actor {
                        |    provider = "akka.remote.RemoteActorRefProvider"
                        |  }
                        |  remote {
                        |    enabled-transports = ["akka.remote.netty.tcp"]
                        |    netty.tcp {
                        |      hostname = ${InetAddress.getLocalHost.getHostAddress()}
                        |      port = 2552
                        |    }
                        |  }
                        |}""".stripMargin

  val esConfig = ConfigFactory.load("querySearch.conf")
  val esHost = esConfig.getString("es.ip")
  val esport = esConfig.getString("es.port").toInt
  val clusterName = esConfig.getString("es.cluster.name")
  val transportClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
    .put("client.transport.sniff", true).build())
  val client = transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esport))
    val config = ConfigFactory.parseString(configString)
  val actorSystem = ActorSystem("twitter", config)
  val router: ActorRef =
    actorSystem.actorOf(RoundRobinPool(2 *
      Runtime.getRuntime.availableProcessors()).props(Props[TwitterQueryFetcher]), "router")

  def main(args: Array[String]): Unit = {
  }

  def fetchAndSaveTweets(term: String, days: String): TermWithCount = {
    val bulkRequest = client.prepareBulk()
    var query = new Query(term).lang("en")
    query.setCount(100)
    query.setSince(days)

    var queryResult = twitter.search(query)
    var tweetCount = 0

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    while (queryResult.hasNext) {
      tweetCount = tweetCount + queryResult.getCount
      queryResult.getTweets.foreach {
        status =>
          val tweetAsJson = TwitterObjectFactory.getRawJSON(status)
          val tmpDoc1 = status.getText.replaceAll("[^\\u0009\\u000a\\u000d\\u0020-\\uD7FF\\uE000-\\uFFFD]", "");
          val tmpDoc2 = tmpDoc1.replaceAll("[\\uD83D\\uFFFD\\uFE0F\\u203C\\u3010\\u3011\\u300A\\u166D\\u200C\\u202A\\u202C\\u2049\\u20E3\\u300B\\u300C\\u3030\\u065F\\u0099\\u0F3A\\u0F3B\\uF610\\uFFFC\\u20B9\\uFE0F]", "");
          val sentiment = SentimentAnalysisUtils.detectSentiment(tmpDoc2)
          val json = parse(tweetAsJson) merge (new JObject(List(("term",JString(term)), ("sentiment",JString(sentiment)))))
          bulkRequest.add(client.prepareIndex("twitter", "tweet").setSource(compact(json)))
      }
      query = queryResult.nextQuery()
      queryResult = twitter.search(query)
    }
    bulkRequest.execute()
    TermWithCount(term, tweetCount)
  }

}

case class QueryTwitter(term: String , days: String)
case class TermWithCount(term: String, count: Int)
case class TermWithCounts(terms : List[TermWithCount])

class TwitterQueryFetcher extends Actor {
  override def receive: Receive = {
    case QueryTwitter(term, days) =>
      sender ! QuerySearch.fetchAndSaveTweets(term, days)
  }
}

object JsonUtils {
  def toJson[T <: AnyRef <% Product with Serializable](t: T): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    import org.json4s.jackson.Serialization.write
    implicit val formats = Serialization.formats(NoTypeHints)
    write(t)
  }
}