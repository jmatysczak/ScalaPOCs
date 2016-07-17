package jmat.scalapocs.server

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.server.TwitterServer
import com.twitter.util.{Await, Future, Time}
import jmat.scalapocs.server.config.responseName

object Main extends TwitterServer {
  val greeting = flag("greeting", "Hello", "The greeting to respond with.")

  val greetingCounter = statsReceiver.counter("service/greeting.count")

  val service = new Service[Request, Response] {
    def apply(request: Request) = {
      greetingCounter.incr()
      log.debug(s"Received a request at ${Time.now}")
      val response = Response(request.version, Status.Ok)
      response.contentString = s"${greeting()} ${responseName()}!"
      Future.value(response)
    }
  }

  def main() {
    val server = Http.serve(":8888", service)

    onExit {
      server.close()
    }

    println()
    println(s"Serving on port ${server.boundAddress}...")
    println(s"Admin server on port ${adminHttpServer.boundAddress}...")
    println(s"Metrics are available at ${adminHttpServer.boundAddress}/admin/metrics.json?pretty=true...")
    println(s"A dynamically loaded admin handler is available at ${adminHttpServer.boundAddress}/admin/handler.json...")

    Await.ready(server)
  }
}