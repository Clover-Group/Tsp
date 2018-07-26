package ru.itclover.streammachine.http.routes

import java.util.concurrent.TimeUnit
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala._
import ru.itclover.streammachine.http.domain.input.FindPatternsRequest
import ru.itclover.streammachine.http.domain.output.{FailureResponse, SuccessfulResponse}
import ru.itclover.streammachine.http.protocols.RoutesProtocols
import ru.itclover.streammachine.io.input.{InfluxDBInputConf, InputConf, JDBCInputConf, RawPattern}
import ru.itclover.streammachine.io.output.{JDBCOutput, JDBCOutputConf, RowSchema}
import ru.itclover.streammachine.transformers._
import ru.itclover.streammachine.DataStreamUtils.DataStreamOps
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import cats.data.Reader
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import ru.itclover.streammachine.utils.CollectionsOps.RightBiasedEither


object JdbcStreamRoutes {
  def fromExecutionContext(implicit strEnv: StreamExecutionEnvironment): Reader[ExecutionContextExecutor, Route] =
    Reader { execContext =>
      new JdbcStreamRoutes {
        implicit val executionContext: ExecutionContextExecutor = execContext
        implicit val streamEnv: StreamExecutionEnvironment = strEnv
      }.route
    }
}


trait JdbcStreamRoutes extends RoutesProtocols {
  implicit val executionContext: ExecutionContextExecutor
  implicit def streamEnv: StreamExecutionEnvironment

  private val log = Logger[JdbcStreamRoutes]

  val route: Route = path("streamJob" / "from-jdbc" / "to-jdbc" /) {
    entity(as[FindPatternsRequest[JDBCInputConf, JDBCOutputConf]]) { patternsRequest =>
      val (inputConf, outputConf, patterns) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patterns)
      log.info(s"Starting patterns finding job with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
        s"patterns codes: `$patterns`")

      val jobIdOrError = for {
        stream <- StreamSources.fromJdbc(inputConf)
        patterns <- PatternsSearchStages.findInRows(stream, inputConf, patterns,
          outputConf.rowSchema)(stream.dataType, streamEnv)
      } yield {
        val chOutputFormat = JDBCOutput.getOutputFormat(outputConf)
        patterns.writeUsingOutputFormat(chOutputFormat).name(s"Writing patterns via JDBC")
        streamEnv.execute(patternsRequest.uuid)
      }

      jobIdOrError match {
        case Right(jobResult) => {
          val execTimeLog = s"Job execution time - ${jobResult.getNetRuntime(TimeUnit.SECONDS)}sec"
          complete(SuccessfulResponse(jobResult.hashCode, Seq(execTimeLog)))
        }
        case Left(err) => failWith(err) // TODO Mb complete(InternalServerError, FailureResponse(5004, err))
      }
    }
  } ~ path("streamJob" / "from-influxdb" / "to-jdbc" /) {
    entity(as[FindPatternsRequest[InfluxDBInputConf, JDBCOutputConf]]) { patternsRequest =>
      val (inputConf, outputConf, patterns) = (patternsRequest.source, patternsRequest.sink, patternsRequest.patterns)
      log.info(s"Starting patterns finding with input JDBC conf: `$inputConf`,\nOutput JDBC conf: `$outputConf`\n" +
        s"patterns codes: `$patterns`")

      val stream = StreamSources.fromInfluxDB(inputConf)
      val jobIdOrError = for {
        patterns <- PatternsSearchStages.findInRows(stream, inputConf, patterns,
          outputConf.rowSchema)(stream.dataType, streamEnv)
      } yield {
        val chOutputFormat = JDBCOutput.getOutputFormat(outputConf)
        patterns.writeUsingOutputFormat(chOutputFormat).name(s"Writing patterns via JDBC")
        streamEnv.execute(patternsRequest.uuid)
      }

      jobIdOrError match {
        case Right(jobResult) => {
          val execTimeLog = s"Job execution time - ${jobResult.getNetRuntime(TimeUnit.SECONDS)}sec"
          complete(SuccessfulResponse(jobResult.hashCode, Seq(execTimeLog)))
        }
        case Left(err) => failWith(err) // TODO Mb complete(InternalServerError, FailureResponse(5004, err))
      }
    }
  }

}