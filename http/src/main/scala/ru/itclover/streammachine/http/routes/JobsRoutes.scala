package ru.itclover.streammachine.http.routes

import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala._
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{
  ExecInfo, FailureResponse, FinishedJobResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.RoutesProtocols
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutput, JDBCOutputConf, OutputConf, RowSchema}
import ru.itclover.streammachine.transformers.PatternsSearchStages
import ru.itclover.streammachine.transformers._
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.types.Row
import ru.itclover.streammachine.{PatternsSearchJob, ResultMapper}
import ru.itclover.streammachine.http.services.flink.MonitoringService
import ru.itclover.streammachine.resultmappers.{ResultMappable, ToRowResultMappers}
import ru.itclover.streammachine.utils.CollectionsOps.RightBiasedEither
import ru.itclover.streammachine.utils.UtilityTypes.ParseException
import scala.util.Success


object JobsRoutes {
  def fromExecutionContext(monitoringUrl: Uri)(implicit strEnv: StreamExecutionEnvironment,
                                               as: ActorSystem,
                                               am: ActorMaterializer): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new JobsRoutes {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
        implicit val actorSystem = as
        implicit val materializer = am
        override val monitoringUri = monitoringUrl
      }.route
    }
}


trait JobsRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment
  implicit val actorSystem: ActorSystem
  implicit val materializer: ActorMaterializer

  val monitoringUri: Uri
  lazy val monitoring = MonitoringService(monitoringUri)

  private val log = Logger[JobsRoutes]

  val route: Route = parameter('run_async.as[Boolean] ? true) { isAsync =>
    path("streamJob" / "from-jdbc" / "to-jdbc"./) {
      entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { request =>
        implicit val src = JdbcSource(request.source)
        val job = new PatternsSearchJob[Row, Any, Row](
          request.source,
          request.sink,
          ToRowResultMappers(request.source.sourceId, request.sink.rowSchema))
        runJob(job, src.emptyEvent, request.patterns, request.uuid, isAsync)
      }
    } ~
      path("streamJob" / "from-influxdb" / "to-jdbc"./) {
        entity(as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]) { request =>
          implicit val src = InfluxDBSource(request.source)
          val job = new PatternsSearchJob[Row, Any, Row](
            request.source,
            request.sink,
            ToRowResultMappers(request.source.sourceId, request.sink.rowSchema))
          runJob(job, src.emptyEvent, request.patterns, request.uuid, isAsync)
        }
      }
   }
  // TODO: Kafka outputConf

  def runJob[InEvent](job: PatternsSearchJob[InEvent, _, _], nullEvent: InEvent, rawPatterns: Seq[RawPattern], uuid: String, runAsync: Boolean = true) = {
    job.preparePhases(rawPatterns) match {
      case Left(ParseException(errs)) =>
        complete(BadRequest, FailureResponse(4001, "Invalid patterns source code", errs))
      case Left(ex) =>
        complete(InternalServerError, FailureResponse(ex))

      case Right(patterns) =>
        log.info(s"Parsed patterns: ${patterns.map(_._1.formatWithInitialState(nullEvent)).mkString(";\n")}")
        if (runAsync) {
          Future { job.findAndSavePatterns(patterns, uuid) }
          complete(SuccessfulResponse(uuid, Seq(s"Job `${uuid}` has started.")))
        } else {
          job.findAndSavePatterns(patterns, uuid) match {
            case Right(result) => {
              val execTime = result.getNetRuntime(TimeUnit.SECONDS)
              // TODO after standalone Flink cluster test work
              onComplete(monitoring.queryJobInfo(uuid)) {
                case Success(Some(details)) => complete(FinishedJobResponse(
                  ExecInfo(execTime, details.getNumRecordsRead, details.getNumProcessedRecords)
                ))
                case _ => complete(FinishedJobResponse(ExecInfo(execTime, None, None)))
              }
            }
            case Left(err) => complete(InternalServerError, FailureResponse(5005, err))
          }
        }
    }

  }
}
