package com.imaginea

import java.text.SimpleDateFormat
import java.util.{Locale, Date}
import com.typesafe.config.ConfigFactory
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{FilterBuilders, FilterBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortOrder, SortBuilders}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

object EsUtils {

  val esConfig = ConfigFactory.load("querySearch.conf")
  val esHost = esConfig.getString("es.ip")
  val esport = esConfig.getString("es.port").toInt
  val clusterName = esConfig.getString("es.cluster.name")
  val transportClient = new TransportClient(ImmutableSettings.settingsBuilder().
    put("cluster.name", clusterName).put("client.transport.sniff", true).build())
  val client = transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esport))

  def convertToDate(dateString: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    format.parse(dateString);
  }

  def getStatus = {
    val searchResponse = client.prepareSearch("tweetedterms")
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).execute().actionGet()
    val termWithStatus  = searchResponse.getHits.map { hit =>
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
    import org.elasticsearch.action.search.SearchType
    import org.elasticsearch.index.query.QueryBuilders
    val queryRequest = client.prepareSearch("tweetedterms").setSearchType(
      SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("searchTerm", term))
    val response = queryRequest.execute.actionGet
    val terms = response.getHits.getHits map { hit =>
      (hit.getSource.get("searchTerm").asInstanceOf[String],
        convertToDate(hit.getSource.get("since").asInstanceOf[String]), "termExists")
    }

    if(!terms.isEmpty) { terms(0) } else {(term, new Date, "newTerm")}
  }
}
