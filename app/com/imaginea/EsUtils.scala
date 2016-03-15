package com.imaginea

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.typesafe.config.ConfigFactory
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}
import org.json4s.JsonAST.JObject
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConversions._

object EsUtils {

  val rj = new RService()
  val esConfig = ConfigFactory.load("querySearch.conf")
  val esHost = esConfig.getString("es.ip")
  val esport = esConfig.getString("es.port").toInt
  val clusterName = esConfig.getString("es.cluster.name")
  val transportClient = new TransportClient(ImmutableSettings.settingsBuilder().
    put("cluster.name", clusterName).put("client.transport.sniff", true).build())
  val client = transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esport))

  def convertToDate(dateString: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
    format.parse(dateString)
  }

  def getStatus = {
    val searchResponse = client.prepareSearch("tweetedterms")
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).execute().actionGet()
    val termWithStatus = searchResponse.getHits.map { hit =>
      val term = hit.getSource.get("searchTerm").asInstanceOf[String]
      val status = hit.getSource.get("queryStatus").asInstanceOf[String]
      TermStatus(term, status)
    }
    JsonUtils.toJson(TermWithStatus(termWithStatus.toList))
  }

  def getTermWithSentiments(term: String) = {
    val sampleTweet = getSampleTweet(term)
    val sentiments = getSentiments(term)
    JsonUtils.toJson(TermWithSentiments(term , sentiments.values.sum.toInt ,sampleTweet, Sentiments(sentiments)))
  }

  def getSentiments(term:String): Map[String, Long] = {
    val searchResponse = client.prepareSearch("twitter")
      .setTypes("tweet").setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.termsQuery("term", term),
        FilterBuilders.notFilter(FilterBuilders.termFilter("sentiment","not_understood"))) )
      .addAggregation(AggregationBuilders.terms("group_by_sentiments").field("sentiment").size(0)).
      execute().get()

    val sentimentsByType = searchResponse.getAggregations.get("group_by_sentiments")
      .asInstanceOf[Terms]
      .getBuckets.map { x => (x.getKey -> x.getDocCount)
    }
    sentimentsByType.toSeq.toMap
}

  def getSampleTweet(term: String) = {
    val queryRequest = client.prepareSearch("twitter").addSort(SortBuilders.fieldSort("created_at").
      order(SortOrder.DESC)).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setSize(20).
      setQuery(QueryBuilders.filteredQuery(QueryBuilders.termsQuery("term", term),
      FilterBuilders.notFilter(FilterBuilders.termFilter("sentiment","not_understood")))).
      setFetchSource(Array("text", "sentiment", "retweet_count", "user.screen_name"), Array[String]())
    val response = queryRequest.execute.actionGet
    val terms: Array[Tweet] = response.getHits.getHits map { hit =>
      val source = hit.getSource
      Tweet(source.get("text").asInstanceOf[String],
        source.get("sentiment").asInstanceOf[String], source.get("retweet_count").asInstanceOf[Int],
        source.get("user").asInstanceOf[java.util.HashMap[String, AnyRef]].get("screen_name").asInstanceOf[String])
    }
    terms.toList
  }

  def shouldIndex(term: String): (String, Date, String) = {
    val queryRequest = client.prepareSearch("tweetedterms").setSearchType(
      SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("searchTerm", term))
    val response = queryRequest.execute.actionGet
    val terms = response.getHits.getHits map { hit =>
      val updateRequest = new UpdateRequest()
      updateRequest.index("tweetedterms")
      updateRequest.id(term)
      updateRequest.doc(jsonBuilder()
        .startObject()
        .field("queryStatus", "refreshing")
        .endObject())
      client.update(updateRequest).get()
      (hit.getSource.get("searchTerm").asInstanceOf[String],
        convertToDate(hit.getSource.get("since").asInstanceOf[String]), "termExists")
    }

    if(!terms.isEmpty) { terms(0) } else {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)
      client.prepareIndex("tweetedterms","typetweetedterms").setSource(jsonBuilder()
        .startObject()
        .field("queryStatus", "pending")
        .field("searchTerm", term)
        .field("lastUpdated",format.format(new Date) )
        .field("since",format.format(new Date))
        .endObject()).execute()
      (term, new Date, "newTerm")}
  }

  def checkTerm(term: String): String= {
    val queryRequest = client.prepareSearch("tweetedterms").setSearchType(
      SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("searchTerm", term))
    val response = queryRequest.execute.actionGet
    var wordJson =""
    val terms = response.getHits.getHits.map { hit =>
      if (!hit.getSource.get("queryStatus").asInstanceOf[String].equalsIgnoreCase("pending")) {
        wordJson=  rj.generateWordCloud(term, esHost, esport).replaceAll("[","").
          replaceAll("]","").replaceAll("String","").trim
        val wordCloudJson = parse(wordJson) merge (new JObject(List(("term", JString(term)))))

        val indexRequest = new IndexRequest("wordcloud", "typewordcloud", term).source(wordCloudJson)
        val updateRequest = new UpdateRequest("wordcloud", "typewordcloud", term).doc(wordCloudJson).upsert(indexRequest)

        EsUtils.client.update(updateRequest).get()
        //EsUtils.client.prepareIndex("wordcloud", "typewordcloud").setSource(wordCloudJson).execute()
      }
    }
    wordJson
  }

  def getWordCloud(term:String) : String = {
    val client : TransportClient  = EsUtils.client
    val searchResponse = client.prepareSearch("wordcloud").setTypes("typewordcloud")
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(QueryBuilders.termsQuery("term", term))
      .execute().actionGet()

      if(searchResponse.getHits.getHits.length > 0){
        val term = searchResponse.getHits.getAt(0).getSource.get("term").asInstanceOf[String]
        val tokens = searchResponse.getHits.getAt(0).getSource.get("tokens").asInstanceOf[java.util.List[String]]
        JsonUtils.toJson(TermsWithTokens(term,tokens))
      }else{
        JsonUtils.toJson(TermsWithTokens(term,Nil))
      }
  }
}

