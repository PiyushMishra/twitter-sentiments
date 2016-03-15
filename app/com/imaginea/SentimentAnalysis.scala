package com.imaginea

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

trait SentimentAnalysis {
  def detectSentiment(text: String) : String
}


class StandfordSentimentImpl extends  SentimentAnalysis{
  val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  val pipeline = new StanfordCoreNLP(nlpProps)

  def detectSentiment(text: String): String = {
    val annotation = pipeline.process(text)
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
}

