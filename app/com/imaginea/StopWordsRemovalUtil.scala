package com.imaginea

/**
 * Created by piyushm on 9/3/16.
 */

import org.ahocorasick.trie._
import scala.collection.JavaConversions._

object StopWordsRemovalUtil extends App {

  val stopWordsList = scala.io.Source.fromFile("src/main/resources/stopwords.txt").getLines()
  var trie: Trie.TrieBuilder = Trie.builder().onlyWholeWords().caseInsensitive()


  def makeStopWordsTrie(stopWordsList: Iterator[String]) = stopWordsList.foreach { stopWord =>
    trie.removeOverlaps().addKeyword(stopWord)
  }

  makeStopWordsTrie(stopWordsList)

  val builder = trie.build()

  def removeStopWordsFromText(text: String) = {
    val finalText = new StringBuffer
    builder.tokenize(text).foreach { token => if (token.isMatch()) {
      finalText.append("");
    } else
      finalText.append(token.getFragment)
    }
    finalText.toString.trim
  }
}
