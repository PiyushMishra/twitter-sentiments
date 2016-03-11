package com.imaginea

import com.imaginea.SentimentAnalysisUtils.SENTIMENT_TYPE
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.Map
import akka.actor.Actor

/**
 * Created by piyushm on 9/3/16.
 */

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


object SentimentAnalysisUtils {
  val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  val pipeline = new StanfordCoreNLP(nlpProps)

  def detectSentiment(message: String): String = {
    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length
    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    /*
     0 -> very negative
     1 -> negative
     2 -> neutral
     3 -> positive
     4 -> very positive
     */
    weightedSentiment match {
      case s if s <= 0.0 => "NOT_UNDERSTOOD"
      case s if s < 1.0 => "VERY_NEGATIVE"
      case s if s < 2.0 => "NEGATIVE"
      case s if s < 3.0 => "NEUTRAL"
      case s if s < 4.0 => "POSITIVE"
      case s if s < 5.0 => "VERY_POSITIVE"
      case s if s > 5.0 => "NOT_UNDERSTOOD"
      case s => "NO SENTIMENT"
    }

  }

  trait SENTIMENT_TYPE
  case object VERY_NEGATIVE extends SENTIMENT_TYPE
  case object NEGATIVE extends SENTIMENT_TYPE
  case object NEUTRAL extends SENTIMENT_TYPE
  case object POSITIVE extends SENTIMENT_TYPE
  case object VERY_POSITIVE extends SENTIMENT_TYPE
  case object NOT_UNDERSTOOD extends SENTIMENT_TYPE

}

/*
class SentimentCalculater extends Actor
{
  def receive : Receive = {
    case text: String =>
      val tmpDoc1 = text.replaceAll("[^\\u0009\\u000a\\u000d\\u0020-\\uD7FF\\uE000-\\uFFFD]", "");
      // remove other unicode characters coreNLP can't handle
      val tmpDoc2 = tmpDoc1.replaceAll("[\\uD83D\\uFFFD\\uFE0F\\u203C\\u3010\\u3011\\u300A\\u166D\\u200C\\u202A\\u202C\\u2049\\u20E3\\u300B\\u300C\\u3030\\u065F\\u0099\\u0F3A\\u0F3B\\uF610\\uFFFC\\u20B9\\uFE0F]", "");
      val sentiment = SentimentAnalysisUtils.detectSentiment(tmpDoc2)
      TweetSentiments(text, sentiment)
    case _ => println("i did not understood the msg")
  }
}*/

object SentimentAnalysis {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Twitter")
    conf.set("esNodesKey", args(0))
    conf.set("esPortKey", args(1))

    import org.elasticsearch.spark._
    val sc: SparkContext = new SparkContext(conf)

    val wordCount: RDD[TweetWithSentiments] = sc.esRDD("twitter/tweet").
      map { case (_, tweet) =>
        val m: Map[String, AnyRef] = tweet
        val text = tweet.get("text").filter(_ != "").get.asInstanceOf[String]
        val sentiment = SentimentAnalysisUtils.detectSentiment(text)
        TweetWithSentiments(tweet.seq, sentiment)
    }

    wordCount.saveToEs("sentiment/typesentiment")
    sc.stop()
  }

}

case class TweetWithSentiments(originalTweet: Map[String, AnyRef], sentiment: String)
