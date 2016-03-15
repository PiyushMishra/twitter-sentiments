package controllers

import com.imaginea.{EsUtils, JsonUtils, TermWithCounts, MasterApp}
import play.api.mvc._
import scala.concurrent.Await
import scala.concurrent.duration._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def analyzeTweet(terms: List[String]) = Action {
    val termToBeUpdated = EsUtils.shouldIndex(terms)
    val finalFuture = MasterApp.process(termToBeUpdated)
    //val list  = Await.result(finalFuture, 50 minutes)
    println("Returning...")
    Ok(terms.toString())
  }

  def searchTerm(term: String) = Action {
    println("Get request...")
    Ok("term : " + term)
  }
}
