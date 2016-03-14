package com.imaginea

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.routing.RoundRobinPool
import com.fasterxml.jackson.annotation.JsonValue
import com.typesafe.config.ConfigFactory
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.{UpdateResponse, UpdateRequest}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import twitter4j._

import scala.collection.JavaConversions._

case class TweetsSentiment(tweet: Status, sentiment: Double)

trait TwitterInstance {
  val twitter = new TwitterFactory(ConfigurationBuilderUtil.buildConfiguration).getInstance()
}


object QuerySearch extends TwitterInstance {
  def configString =
    s"""akka {|  actor {|    provider = "akka.remote.RemoteActorRefProvider"|  }|  remote {|    enabled-transports = ["akka.remote.netty.tcp"]|    netty.tcp {|      hostname = ${InetAddress.getLocalHost.getHostAddress()}|      port = 2552|    }|  }|}""".stripMargin

  val config = ConfigFactory.parseString(configString)
  val actorSystem = ActorSystem("twitter", config)
  val router: ActorRef =
    actorSystem.actorOf(RoundRobinPool(2 *
      Runtime.getRuntime.availableProcessors()).props(Props[TwitterQueryFetcher]), "router")

  def main(args: Array[String]): Unit = {
  }


  def fetchAndSaveTweets(terms: (String, String), days: String): TermWithCount = {
    val (term, since) = terms
    if (since.isEmpty) {
      indexTweetedTerms(term, "pending", new Date)
    } else {
      indexTweetedTerms(term, "refreshing", new Date(since))
    }
    val bulkRequest = EsUtils.client.prepareBulk()
    var query = new Query(term).lang("en")
    query.setCount(100)

    if (!since.isEmpty) {
      query.setSince(since)
    } else {
      query.setSince(days)
    }

    var queryResult = twitter.search(query)
    var tweetCount = 0
    var recentTweet = false
    var createdAt = new Date

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    while (queryResult.hasNext) {
      tweetCount = tweetCount + queryResult.getCount
      queryResult.getTweets.foreach { status =>
        if (!recentTweet) {
          createdAt = status.getCreatedAt
          recentTweet = true
        }
        val tweetAsJson = TwitterObjectFactory.getRawJSON(status)
        val tmpDoc1 = status.getText.replaceAll("[^\\u0009\\u000a\\u000d\\u0020-\\uD7FF\\uE000-\\uFFFD]", "")
        val tmpDoc2 = tmpDoc1.replaceAll("[\\uD83D\\uFFFD\\uFE0F\\u203C\\u3010\\u3011\\u300A\\u166D\\u200C\\u202A\\u202C\\u2049\\u20E3\\u300B\\u300C\\u3030\\u065F\\u0099\\u0F3A\\u0F3B\\uF610\\uFFFC\\u20B9\\uFE0F]", "");
        val sentiment = SentimentAnalysisUtils.detectSentiment(tmpDoc2)
        val json = parse(tweetAsJson) merge (new JObject(List(("term", JString(term)), ("sentiment", JString(sentiment)))))
        bulkRequest.add(EsUtils.client.prepareIndex("twitter", "tweet").setSource(compact(json)))
      }
      query = queryResult.nextQuery()
      queryResult = twitter.search(query)
    }
    bulkRequest.execute()
    if (tweetCount == 0 && since.isEmpty) {
      EsUtils.client.prepareDelete("tweetedterms", "typetweetedterms", term).execute()
    } else {
      indexTweetedTerms(term, "done", createdAt)
    }

    TermWithCount(term, tweetCount)
  }

  def indexTweetedTerms(term: String, status: String, createdAt: Date): UpdateResponse = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val indexRequest = new IndexRequest("tweetedterms", "typetweetedterms", term).source(JsonUtils.toJson(TweetedTerms(term, status, sdf.format(new Date), sdf.format(createdAt))))
    val updateRequest = new UpdateRequest("tweetedterms", "typetweetedterms", term).doc(JsonUtils.toJson(TweetedTerms(term, status, sdf.format(new Date), sdf.format(createdAt)))).upsert(indexRequest)
    EsUtils.client.update(updateRequest).get()
  }
}

case class QueryTwitter(term: (String, String), days: String)

case class TermWithCount(term: String, count: Int)

case class TermWithCounts(terms: List[TermWithCount])

case class TweetedTerms(searchTerm: String, queryStatus: String, lastUpdated: String, since: String)

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

object EsUtils {

  val esConfig = ConfigFactory.load("querySearch.conf")
  val esHost = esConfig.getString("es.ip")
  val esport = esConfig.getString("es.port").toInt
  val clusterName = esConfig.getString("es.cluster.name")
  val transportClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.sniff", true).build())
  val client = transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esport))

  def shouldIndex2(term: List[String]) = {
    var finalList = term
    import org.elasticsearch.action.search.SearchType;
    import org.elasticsearch.index.query.QueryBuilders
    val response = client.prepareSearch("tweetedterms").setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termsQuery("terms", term)).execute().actionGet()

    for (hit <- response.getHits.getHits) {
      val lastUpdated = hit.getSource.get("last_updated").asInstanceOf[java.util.Date]
      val currentDate = new Date()
      val diff = currentDate.getTime() - lastUpdated.getTime()
      if (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) < 7 || "pending".
        equalsIgnoreCase(hit.getSource.get("queryStatus").asInstanceOf[String]))
        finalList = finalList.drop(term.indexOf(hit.getSource.get("term").asInstanceOf[String]))
    }
    finalList
  }

  def shouldIndex(term: List[String]): List[(String, String)] = {
    import scala.collection.JavaConverters._
    var finalList = term
    import org.elasticsearch.action.search.SearchType;
    import org.elasticsearch.index.query.QueryBuilders
    val queryRequest = client.prepareSearch("tweetedterms").
      setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termsQuery("searchTerm", term.asJava))
//    println(queryRequest.toString)
    val response = queryRequest.execute.actionGet
    val terms = response.getHits.getHits.map { hit =>
      finalList = finalList.drop(term.indexOf(hit.getSource.get("SearchTerm").asInstanceOf[String]))
      println("finalList : " + finalList)
     // println(hit.getSource.get("searchTerm").asInstanceOf[String] + " :: MAPPING :: " +
      //  hit.getSource.get("since").asInstanceOf[String])
      (hit.getSource.get("searchTerm").asInstanceOf[String], hit.getSource.get("since").asInstanceOf[String])
    }
    val a = finalList.map(term => (term, "")) ++ terms
    println(a)
    a
  }
}