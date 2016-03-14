package controllers

import com.imaginea.{JsonUtils, TermWithCounts, MasterApp}
import play.api.mvc._
import scala.concurrent.Await
import scala.concurrent.duration._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def analyzeTweet = Action { request =>
    println("Content : " + request.body.toString())
    val json = request.body.asJson.get
    val days = json \ "days"
    val terms = json \ "terms"
    val finalFuture = MasterApp.process(terms.as[List[String]], days.as[String])
    val list  = Await.result(finalFuture, 50 minutes)
    println("got result  " + list)
    Ok(JsonUtils.toJson(TermWithCounts(list)))
  }

  def searchTerm(term: String) = Action {
    Ok("term : " + term)
  }
}
