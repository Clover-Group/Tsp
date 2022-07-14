package ru.itclover.tsp.http.services.queuing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.StreamSource.Row
import ru.itclover.tsp.core.{Incident, RawPattern}
import ru.itclover.tsp.{JdbcSource, KafkaSource, RowWithIdx, StreamSource}
import ru.itclover.tsp.core.io.{AnyDecodersInstances, BasicDecoders}
import ru.itclover.tsp.dsl.PatternFieldExtractor
import ru.itclover.tsp.http.domain.input.{FindPatternsRequest, QueueableRequest}
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.services.streaming.MonitoringServiceModel.{JobDetails, Vertex, VertexMetrics}
import ru.itclover.tsp.streaming.io.{InputConf, JDBCInputConf, KafkaInputConf}
import ru.itclover.tsp.streaming.io.{JDBCOutputConf, KafkaOutputConf, OutputConf}
import ru.itclover.tsp.streaming.mappers.PatternsToRowMapper
import ru.itclover.tsp.streaming.PatternsSearchJob
import ru.itclover.tsp.streaming.utils.ErrorsADT
import ru.itclover.tsp.streaming.utils.ErrorsADT.RuntimeErr
import spray.json._

import java.nio.file.Paths
import java.time.LocalDateTime
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import collection.JavaConverters._


class QueueManagerService(id: String, blockingExecutionContext: ExecutionContextExecutor)(
  implicit executionContext: ExecutionContextExecutor,
  actorSystem: ActorSystem,
  materializer: ActorMaterializer,
  decoders: BasicDecoders[Any] = AnyDecodersInstances
) extends SprayJsonSupport
    with DefaultJsonProtocol {
  import ru.itclover.tsp.http.services.AkkaHttpUtils._

  type TypedRequest = (QueueableRequest, String)
  type Request = FindPatternsRequest[RowWithIdx, Symbol, Any, Row]


  case class Metric(id: String, value: String)

  implicit val metricFmt = jsonFormat2(Metric.apply)

  private val log = Logger[QueueManagerService]

  //val jobQueue = PersistentSet[TypedRequest, Nothing, Glass](dir = Paths.get("/tmp/job_queue"))
  //log.warn(s"Recovering job queue: ${jobQueue.count} entries found")
  val jobQueue = mutable.Queue[TypedRequest]()

  val isLocalhost: Boolean = true

  val ex = new ScheduledThreadPoolExecutor(1)

  val task: Runnable = new Runnable {
    def run(): Unit = onTimer()
  }
  val f: ScheduledFuture[_] = ex.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS)
  //f.cancel(false)

  def enqueue(r: Request): Unit = {
    jobQueue.enqueue(
      (r,
        confClassTagToString(ClassTag(r.inputConf.getClass))
      )
      )
    log.info(s"Job ${r.uuid} enqueued.")
  }

  def confClassTagToString(ct: ClassTag[_]): String = ct.runtimeClass match {
    case c if c.isAssignableFrom(classOf[JDBCInputConf]) => "from-jdbc"
    case c if c.isAssignableFrom(classOf[KafkaInputConf]) => "from-kafka"
    case _ => "unknown"
  }

  def getQueuedJobs: Seq[QueueableRequest] = jobQueue.map(_._1).toSeq

  def runJdbc(request: Request): Unit = {
    log.info("JDBC-to-JDBC: query started")
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)
    log.info("JDBC-to-JDBC: extracted fields from patterns. Creating source...")
    val resultOrErr = for {
      source <- JdbcSource.create(inputConf.asInstanceOf[JDBCInputConf], fields)
      _ = log.info("JDBC-to-JDBC: source created. Creating patterns stream...")
      streams <- createStream(patterns, inputConf, outConf, source)
      _ = log.info("JDBC-to-JDBC: stream created. Starting the stream...")
      result <- runStream(uuid, streams)
      _ = log.info("JDBC-to-JDBC: stream started")
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }

  def runKafka(request: Request): Unit = {
    import request._
    val fields: Set[Symbol] = PatternFieldExtractor.extract(patterns)

    val resultOrErr = for {
      source <- KafkaSource.create(inputConf.asInstanceOf[KafkaInputConf], fields)
      _ = log.info("Kafka create done")
      streams <- createStream(patterns, inputConf, outConf, source)
      _ = log.info("Kafka createStream done")
      result <- runStream(uuid, streams)
      _ = log.info("Kafka runStream done")
    } yield result
    resultOrErr match {
      case Left(error) => log.error(s"Cannot run request. Reason: $error")
      case Right(_)    => log.info(s"Stream successfully started!")
    }
  }


  /*def dequeueAndRun(slots: Int): Unit = {
    // TODO: Functional style
    var slotsRemaining = slots
    while (jobQueue.nonEmpty && slotsRemaining >= jobQueue.head._1.requiredSlots) {
      val request = jobQueue.dequeue()
      slotsRemaining -= request._1.requiredSlots
      run(request)
    }
  }*/

  def dequeueAndRunSingleJob(): Unit = {
    val request = jobQueue.head
    jobQueue.dequeue()
    run(request)
  }

  def removeFromQueue(uuid: String): Option[Unit] = {
    val job = jobQueue.find(_._1.uuid == uuid)
    job match {
      case Some(value) => {
        //jobQueue.remove(value)
        Some(())
      }
      case None => None
    }
  }

  def queueAsScalaSeq: Seq[QueueableRequest] = jobQueue.map(_._1).toSeq

  def run(typedRequest: TypedRequest): Unit = {
    val (r, inClass) = typedRequest
    val request = r.asInstanceOf[Request]
    log.info(s"Dequeued job ${request.uuid}, sending")
    inClass match {
      case "from-jdbc" =>
        runJdbc(request)
      case "from-kafka" =>
        runKafka(request)
      case _ =>
        log.error(s"Unknown job request type: IN: $inClass")
    }
  }

  type EKey = Symbol

  def createStream[E, EItem](
    patterns: Seq[RawPattern],
    inputConf: InputConf[E, EKey, EItem],
    outConf: Seq[OutputConf[Row]],
    source: StreamSource[E, EKey, EItem]
  )(implicit decoders: BasicDecoders[EItem]): Either[ErrorsADT.ConfigErr, Seq[fs2.Stream[IO, Unit]]] = {

    log.debug("createStream started")

    val searcher = PatternsSearchJob(source, decoders)
    val strOrErr = searcher.patternsSearchStream(
      patterns,
      outConf,
      outConf.map(conf => PatternsToRowMapper[Incident, Row](conf.rowSchema))
    )
    strOrErr.map {
      case (parsedPatterns, stream) =>
        // .. patternV2.format
        val strPatterns = parsedPatterns.map {
          case ((_, meta), _) =>
            /*p.format(source.emptyEvent) +*/
            s" ;; Meta=$meta"
        }
        log.debug(s"Parsed patterns:\n${strPatterns.mkString(";\n")}")
        stream
    }
  }

  def runStream(uuid: String, streams: Seq[fs2.Stream[IO, Unit]]): Either[RuntimeErr, Option[String]] = {
    log.debug("runStream started")

    // Run the streams (multiple sinks)
    streams.foreach { stream =>
      stream.compile.drain.unsafeRunAsync {
        case Left(throwable) =>
          // TODO: Report throwable
          log.error(s"Job $uuid failed: $throwable")
        case Right(_) =>
          // success
          log.info(s"Job $uuid finished")
      }
    }

    log.debug("runStream finished")
    Right(None)
  }

  def availableSlots: Future[Int] = Future(32)


  def onTimer(): Unit = {
    availableSlots.onComplete {
      case Success(slots) =>
        if (slots > 0 && jobQueue.nonEmpty) {
          log.info(s"$slots slots available")
          dequeueAndRunSingleJob()
        } else {
          if (jobQueue.nonEmpty)
            log.info(
              s"Waiting for free slot ($slots available), cannot run jobs right now"
            )
        }
      case Failure(exception) =>
        log.warn(s"An exception occurred when checking available slots: $exception --- ${exception.getMessage}")
        log.warn("Trying to send job anyway (assuming 1 available slot)...")
        dequeueAndRunSingleJob()
    }

  }
}

object QueueManagerService {
  val services: mutable.Map[Uri, QueueManagerService] = mutable.Map.empty

  def getOrCreate(id: String, blockingExecutionContext: ExecutionContextExecutor)(
    implicit executionContext: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    materializer: ActorMaterializer,
    decoders: BasicDecoders[Any] = AnyDecodersInstances
  ): QueueManagerService = {
    if (!services.contains(id)) services(id) = new QueueManagerService(id, blockingExecutionContext)
    services(id)
  }
}
