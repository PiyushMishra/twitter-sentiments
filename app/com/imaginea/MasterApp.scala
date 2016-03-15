package com.imaginea

import java.util.Date

import akka.actor._
import com.typesafe.config.ConfigFactory


object MasterApp {
  val actorSystem = ActorSystem("twitter")
  val querySearchConfig = ConfigFactory.load("querySearch.conf")

  def process(term: (String, Date, String)) =  {
     QuerySearch.router ! QueryTwitter(term, "")
  }
}