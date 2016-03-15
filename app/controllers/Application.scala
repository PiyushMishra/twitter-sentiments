package controllers

import com.imaginea.{EsUtils, JsonUtils, TermWithCounts, MasterApp}
import play.api.mvc._
import scala.concurrent.Await
import scala.concurrent.duration._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def analyzeTweet(term: String) = Action {
    val termToBeUpdated = EsUtils.shouldIndex(term)
    MasterApp.process(termToBeUpdated)
    Ok(term)
  }

  def searchTerm(term: String) = Action {
    Ok(EsUtils.getTermWithSentiments(term))
  }

  def getStatus = Action {
    Ok(EsUtils.getStatus)
  }
}
