package jmat.scalapocs.httpmuxhandler

import com.twitter.finagle.http.{HttpMuxHandler}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.util.Future

class Handler extends HttpMuxHandler {
  val pattern: String = "/admin/handler.json"

  def apply(request: Request) = {
    val response = Response(request.version, Status.Ok)
    response.contentString = "{\"message\":\"Hello Handler!\"}"
    Future.value(response)
  }
}
