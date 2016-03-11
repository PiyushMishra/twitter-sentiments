package controllers

import com.imaginea.MasterApp
import play.api.mvc._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def analyzeTweet = Action { request =>
    println("Content : "+request.body.toString())
    val json = request.body.asJson.get
    val days = json \ "days"
    val terms = json \ "terms"
    MasterApp.process(terms.as[List[String]], days.as[String])
    Ok("Days : "+days+ " :: Terms : "+terms)
  }

  def searchTerm(term:String) = Action{
    Ok("term : "+term)
  }
}