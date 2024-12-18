package ru.itclover.tsp.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes.{LoopDetected, BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import cats.data.Reader
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp._
import ru.itclover.tsp.core.io.AnyDecodersInstances
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.queuing.JobRunService
import spray.json._

import scala.concurrent.ExecutionContextExecutor
import ru.itclover.tsp.http.Launcher
import ru.itclover.tsp.http.domain.output.FailureResponse
import ru.itclover.tsp.streaming.utils.ErrorsADT.JobLimitErr
import ru.itclover.tsp.streaming.utils.ErrorsADT.ConfigErr
import ru.itclover.tsp.streaming.utils.ErrorsADT.RuntimeErr

trait JobsRoutes extends RoutesProtocols:
  implicit val executionContext: ExecutionContextExecutor
  val blockingExecutionContext: ExecutionContextExecutor
  implicit val actorSystem: ActorSystem
  implicit val materializer: Materializer
  implicit val decoders: AnyDecodersInstances.type = AnyDecodersInstances

  val jobRunService: JobRunService

  Logger[JobsRoutes]

  implicit object statusFormat extends JsonFormat[Map[String, String | Int]]:

    override def read(json: JsValue): Map[String, String | Int] = json match
      case JsObject(fields) =>
        fields.map { case (k, v) =>
          v match
            case JsNumber(value) => (k, value.toIntExact)
            case JsString(value) => (k, value)
            case _               => throw DeserializationException(s"Value must be string or number, but $v found")
        }
      case _ => throw DeserializationException(s"Value must be object, but $json found")

    override def write(obj: Map[String, String | Int]): JsValue = JsObject(obj.map {
      case (k, v: String) => (k, JsString(v))
      case (k, v: Int)    => (k, JsNumber(v))
    })

  val route: Route =
    path("job" / "submit"./):
      entity(as[FindPatternsRequest[RowWithIdx, String, Any, Row]]) { request =>
        val result = jobRunService.process(request)
        result match
          case Right(_) =>
            complete(
              Map(
                "status"            -> s"Job ${request.uuid} received.",
                "currentJobsCount"  -> jobRunService.currentJobsCount.get(),
                "finishedJobsCount" -> jobRunService.finishedJobsCount.get(),
                "maxJobsCount"      -> jobRunService.maxJobsCount
              ).toJson
            )
          case Left(e) =>
            e match
              case jle: JobLimitErr => complete((LoopDetected, FailureResponse(jle.errorCode, jle.error, Seq.empty)))
              case ce: ConfigErr    => complete((BadRequest, FailureResponse(ce)))
              case re: RuntimeErr   => complete((InternalServerError, FailureResponse(re)))
              case null             => complete((InternalServerError, FailureResponse(5990, "Unknown error", Seq.empty)))
      }

object JobsRoutes:

  private val log = Logger[JobsRoutes]

  def fromExecutionContext(blocking: ExecutionContextExecutor)(implicit
    as: ActorSystem,
    am: Materializer
  ): Reader[ExecutionContextExecutor, Route] =

    log.debug("fromExecutionContext started")

    Reader { execContext =>
      new JobsRoutes {
        val blockingExecutionContext = blocking
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val actorSystem = as
        implicit val materializer = am
        override val jobRunService = JobRunService.getOrCreate("mgr", Launcher.getMaxTotalJobCount, blocking)(
          execContext,
          as,
          am,
          AnyDecodersInstances
        )
      }.route
    }

  log.debug("fromExecutionContext finished")
