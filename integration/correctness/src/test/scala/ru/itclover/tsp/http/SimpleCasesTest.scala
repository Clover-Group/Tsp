package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers._
import fs2.kafka.{Acks, KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer}
import ru.itclover.tsp.http.services.queuing.JobRunService
import ru.itclover.tsp.streaming.io.{IntESValue, StringESValue}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

//import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.UUID
import org.scalatest.Assertion
import org.scalatest.flatspec._

import scala.util.Failure
// import org.scalatest.concurrent.Waiters._
// import org.testcontainers.containers.Network
import ru.itclover.tsp.core.RawPattern
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.protocols.RoutesProtocols
import ru.itclover.tsp.http.utils.SqlMatchers
import ru.itclover.tsp.streaming.io.{JDBCInputConf, KafkaInputConf, NarrowDataUnfolding, WideDataFilling}
import ru.itclover.tsp.streaming.io.{JDBCOutputConf, NewRowSchema}
import ru.itclover.tsp.streaming.utils.Files
import spray.json._

import scala.annotation.tailrec
import scala.util.{Success, Try}
// import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import ru.itclover.tsp.streaming.io.KafkaOutputConf
import ru.itclover.tsp.streaming.checkpointing.CheckpointingService
import ru.itclover.tsp.dsl.PatternsValidatorConf
import ru.itclover.tsp.http.protocols.PatternsValidatorProtocols
import ru.itclover.tsp.http.domain.output.FailureResponse
import org.testcontainers.utility.DockerImageName

// In test cases, 'should' expressions are non-unit. Suppressing wartremover warnings about it
// Also, this test seems to be heavily relying on Any. But still TODO: Investigate
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Any"))
class SimpleCasesTest
    extends AnyFlatSpec
    with SqlMatchers
    with ScalatestRouteTest
    with HttpService
    with ForAllTestContainer
    with RoutesProtocols
    with PatternsValidatorProtocols:
  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global

  override val jobRunService: JobRunService = JobRunService.getOrCreate("mgr", 5000, executionContext)

  // to run blocking tasks.
  val blockingExecutorContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(
        0, // corePoolSize
        Int.MaxValue, // maxPoolSize
        1000L, // keepAliveTime
        TimeUnit.MILLISECONDS, // timeUnit
        new SynchronousQueue[Runnable]() // workQueue
        // new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  implicit def defaultTimeout: RouteTestTimeout = RouteTestTimeout(300.seconds)

  val port = 8161
  val influxPort = 8144
  val chNativePort = 9098
  val coordinatorPort = 8181
  val redisPort = 16379

  implicit val clickhouseContainer: ClickHouseContainer = ClickHouseContainer(
    DockerImageName.parse("clickhouse/clickhouse-server:23.2")
  )

  val wrongJdbcUrl = "jdbc:clickhouse://10.10.10.10:8123/default"

  val kafkaContainer = KafkaContainer()

  val redisContainer = RedisContainer(DockerImageName.parse("valkey/valkey:8.0.1"))
  redisContainer.container.setPortBindings(List(s"$redisPort:6379").asJava)

  val coordinatorContainer = GenericContainer("clovergrp/tsp-coordinator:1.3.13")
  coordinatorContainer.underlyingUnsafeContainer.setPortBindings(List(s"$coordinatorPort:5000").asJava)

  val submitUrl = "/job/submit"

  override val container = MultipleContainers(
    clickhouseContainer,
    kafkaContainer,
    coordinatorContainer,
    redisContainer
  )

  lazy val kafkaBrokerUrl = kafkaContainer.bootstrapServers

  val filesPath = "integration/correctness/src/test/resources/simple_cases"

  val patternsPath = s"${filesPath}/core/patterns.json"

  val patternsString: Try[String] = Files.readFile(patternsPath)

  val fileSourceString = patternsString match
    case Success(some) => some
    case _             => ""

  val jsonObject = fileSourceString.parseJson
  val casesPatterns = jsonObject.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  val patternsPathIvolga = s"${filesPath}/ivolga/patterns.json"

  val patternsStringIvolga: Try[String] = Files.readFile(patternsPathIvolga)

  val fileSourceStringIvolga = patternsStringIvolga match
    case Success(some) => some
    case _             => ""

  val jsonObjectIvolga = fileSourceStringIvolga.parseJson
  val casesPatternsIvolga = jsonObjectIvolga.convertTo[Seq[RawPattern]].map(p => (p.id -> p)).toMap

  val coreIncidentsPath = s"${filesPath}/core/incidents.json"
  val incidentsString: Try[String] = Files.readFile(coreIncidentsPath)

  val fileSourceStringInc = incidentsString match
    case Success(some) => some
    case _             => ""

  val jsonObjectInc = fileSourceStringInc.parseJson
  val coreRawIncidents = jsonObjectInc.convertTo[Map[String, String]]

  val incidentsCount = coreRawIncidents.map { case (k, v) => (k.toInt, v.toInt) }

  val ivolgaIncidentsPath = s"${filesPath}/ivolga/incidents.json"
  val ivolgaIncidentsString: Try[String] = Files.readFile(ivolgaIncidentsPath)

  val fileSourceStringIvolgaInc = ivolgaIncidentsString match
    case Success(some) => some
    case _             => ""

  val jsonObjectIvolgaInc = fileSourceStringIvolgaInc.parseJson
  val ivolgaIncidents = jsonObjectIvolgaInc.convertTo[Map[String, String]]

  val incidentsIvolgaCount = ivolgaIncidents.map { case (k, v) => (k.toInt, v.toInt) }

  val rawIncidentsTimestamps: ListBuffer[List[Double]] = ListBuffer.empty

  Files
    .readResource("/simple_cases/core/timestamps.csv")
    .foreach(elem => {

      val elements = elem.split(",")
      rawIncidentsTimestamps += List(
        elements(0).toDouble,
        elements(1).toDouble,
        elements(2).toDouble
      )

    })

  // type is explicitly specified to avoid writing pattern ID as Double
  val incidentsTimestamps: List[List[Double]] = rawIncidentsTimestamps.toList

  val ivolgaIncidentsTimestamps: ListBuffer[List[Double]] = ListBuffer.empty

  Files
    .readResource("/simple_cases/ivolga/timestamps.csv")
    .foreach(elem => {

      val elements = elem.split(",")
      ivolgaIncidentsTimestamps += List(
        elements(0).toDouble,
        elements(1).toDouble,
        elements(2).toDouble
      )

    })

  val incidentsIvolgaTimestamps: List[List[Double]] = ivolgaIncidentsTimestamps.toList

  lazy val wideInputConf = JDBCInputConf(
    sourceId = 100,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    query = "SELECT * FROM `2te116u_tmy_test_simple_rules` ORDER BY ts",
    driverName = clickhouseContainer.driverClassName,
    datetimeField = "ts",
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    processingBatchSize = Some(10000),
    unitIdField = Some("loco_num"),
    partitionFields = Seq("loco_num", "section", "upload_id"),
    userName = Some("default")
  )

  lazy val narrowInputConf = wideInputConf.copy(
    sourceId = 200,
    query = "SELECT * FROM math_test ORDER BY dt",
    datetimeField = "dt",
    dataTransformation = Some(NarrowDataUnfolding("sensor_id", "value_float", Map.empty, Some(Map.empty), Some(1000)))
  )

  lazy val narrowInputIvolgaConf = wideInputConf.copy(
    sourceId = 400,
    query = "SELECT * FROM ivolga_test_narrow ORDER BY dt",
    datetimeField = "dt",
    unitIdField = Some("stock_num"),
    partitionFields = Seq("stock_num", "upload_id"),
    dataTransformation = Some(
      NarrowDataUnfolding(
        "sensor_id",
        "value_float",
        Map.empty,
        Some(Map("value_str" -> List("SOC_2_UKV1_UOVS"))),
        Some(15000L)
      )
    )
  )

  lazy val wideInputIvolgaConf = wideInputConf.copy(
    sourceId = 500,
    query = "SELECT * FROM `ivolga_test_wide` ORDER BY ts",
    driverName = "ru.yandex.clickhouse.ClickHouseDriver",
    unitIdField = Some("stock_num"),
    partitionFields = Seq("stock_num", "upload_id"),
    dataTransformation = Some(WideDataFilling(Map.empty, defaultTimeout = Some(15000L)))
  )

  val wideRowSchema = NewRowSchema(
    Map(
      "series_storage" -> StringESValue("int32", "$Unit"),
      "from"           -> StringESValue("timestamp", "$IncidentStart"),
      "to"             -> StringESValue("timestamp", "$IncidentEnd"),
      "app"            -> IntESValue("int32", 1),
      "id"             -> StringESValue("int32", "$PatternID"),
      "subunit"        -> StringESValue("int32", "$Subunit"),
      "uuid"           -> StringESValue("string", "$UUID")
    )
  )

  lazy val wideKafkaInputConf = KafkaInputConf(
    sourceId = 600,
    brokers = kafkaBrokerUrl,
    topic = "2te116u_tmy_test_simple_rules",
    datetimeField = "dt",
    unitIdField = Some("loco_num"),
    partitionFields = Seq("loco_num", "section", "upload_id"),
    processingBatchSize = Some(10000),
    fieldsTypes = Map(
      "dt"             -> "float64",
      "upload_id"      -> "string",
      "loco_num"       -> "string",
      "section"        -> "string",
      "POilDieselOut"  -> "float64",
      "SpeedThrustMin" -> "float64",
      "PowerPolling"   -> "float64"
    )
  )

  val narrowRowSchema = wideRowSchema.copy(
    data = wideRowSchema.data.updated("app", IntESValue("int32", 2))
  )

  val narrowIvolgaRowSchema = wideRowSchema.copy(
    data = wideRowSchema.data.updated("app", IntESValue("int32", 3))
  )

  val wideIvolgaRowSchema = wideRowSchema.copy(
    data = wideRowSchema.data.updated("app", IntESValue("int32", 4))
  )

  val wideKafkaRowSchema =
    wideRowSchema

  val wideToKafkaRowSchema =
    wideRowSchema

  lazy val wideOutputConf = JDBCOutputConf(
    tableName = "events_wide_test",
    rowSchema = wideRowSchema,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    driverName = clickhouseContainer.driverClassName,
    userName = Some("default")
    // password = Some("")
  )

  lazy val narrowOutputConf = wideOutputConf.copy(
    tableName = "events_narrow_test",
    rowSchema = narrowRowSchema
  )

  lazy val narrowOutputIvolgaConf = wideOutputConf.copy(
    tableName = "events_narrow_ivolga_test",
    rowSchema = narrowIvolgaRowSchema
  )

  lazy val wideOutputIvolgaConf = wideOutputConf.copy(
    tableName = "events_wide_ivolga_test",
    rowSchema = wideIvolgaRowSchema
  )

  lazy val wideKafkaOutputConf = JDBCOutputConf(
    tableName = "events_wide_kafka_test",
    rowSchema = wideKafkaRowSchema,
    jdbcUrl = clickhouseContainer.jdbcUrl,
    driverName = clickhouseContainer.driverClassName,
    userName = Some("default")
  )

  lazy val wideToKafkaOutputConf = KafkaOutputConf(
    broker = kafkaBrokerUrl,
    topic = "2te116u_tmy_test_events",
    rowSchema = wideToKafkaRowSchema
  )

  override def afterStart(): Unit =
    super.afterStart()

    val chScripts: Seq[String] = Seq(
      "/sql/test/cases-narrow-schema-new.sql",
      "/sql/test/cases-wide-schema-new.sql",
      "/sql/test/cases-narrow-schema-ivolga.sql",
      "/sql/test/cases-wide-schema-ivolga.sql",
      "/sql/test/cases-sinks-schema.sql"
    )

    chScripts.foreach(elem => {

      Files
        .readResource(elem)
        .mkString
        .split(";")
        .foreach(q => {
          val con = clickhouseContainer.container.createConnection("")
          con.prepareStatement(q).executeUpdate()
        })

    })

    val insertInfo = Seq(
      ("math_test", "/sql/test/cases-narrow-new.csv"),
      ("ivolga_test_narrow", "/sql/test/cases-narrow-ivolga.csv"),
      ("ivolga_test_wide", "/sql/test/cases-wide-ivolga.csv"),
      ("`2te116u_tmy_test_simple_rules`", "/sql/test/cases-wide-new.csv")
    )

    val producerSettings = ProducerSettings(
      keySerializer = Serializer[IO, String],
      valueSerializer = Serializer[IO, String]
    ).withBootstrapServers(kafkaBrokerUrl)
      .withRetries(2)
      .withLinger(FiniteDuration.apply(1, TimeUnit.MILLISECONDS))
      .withAcks(Acks.All)
      .withProperty("auto.create.topics.enable", "true")

    insertInfo.foreach(elem => {

      val insertData = Files
        .readResource(elem._2)
        .drop(1)
        .mkString("\n")

      clickhouseContainer.container
        .createConnection("")
        .prepareStatement(s"INSERT INTO ${elem._1} FORMAT CSV\n${insertData}")
        .executeUpdate()

      val headers = Files.readResource(elem._2).take(1).toList.headOption.getOrElse("").split(",")
      val data = Files.readResource(elem._2).drop(1).map(_.split(",")).toArray
      val numberIndices =
        List("dt", "ts", "POilDieselOut", "SpeedThrustMin", "PowerPolling", "value_float").map(headers.indexOf(_))

      fs2.Stream
        .emits(data)
        .map { row =>
          val convertedRow: Seq[Any] = row.indices.map(idx =>
            if numberIndices.contains(idx) then {
              if row(idx) == "\\N" then Double.NaN else row(idx).toDouble
            } else row(idx)
          )
          val msgKey = UUID.randomUUID().toString
          val msgMap = headers.zip(convertedRow).toMap[String, Any]
          val json = "{" + msgMap
            .map { case (k, v) =>
              v match {
                case _: String => s""""$k": "$v""""
                case _         => s""""$k": $v"""
              }
            }
            .mkString(", ") + "}"
          val topic = elem._1.filter(_ != '`')
          println(s"Sending to $topic $msgKey --- $json")
          val rec = ProducerRecord(topic, msgKey, json)
          ProducerRecords.one(rec)
        }
        .through(KafkaProducer.pipe(producerSettings))
        .compile
        .drain
        .unsafeRunSync()

    })

  def firstValidationQuery(table: String, numbers: Seq[Range]) = s"""
       SELECT number, c
       FROM (
         ${numbers.map(r => s"SELECT number FROM numbers(${r.start}, ${r.size})").mkString(" UNION ALL ")}
       ) num
       LEFT JOIN (
         SELECT id, COUNT(id) AS c FROM ${table} GROUP BY id
       ) e ON num.number = e.id ORDER BY num.number
  """

  val secondValidationQuery = "SELECT id, toUnixTimestamp(from) AS from_ts, " +
    "toUnixTimestamp(to) AS to_ts FROM %s ORDER BY id, from_ts, to_ts"

  override def afterAll(): Unit =
    super.afterAll()
    clickhouseContainer.stop()
    container.stop()

  // Here, default argument for `epsilon` is useful.
  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def alertByQuery(expectedValues: Seq[Seq[Double]], query: String, epsilon: Double = 0.0001): Assertion =
    Try(checkByQuery(expectedValues, query, epsilon)) match
      case Failure(exception) =>
        alert(exception.getMessage)
        assert(true)
      case Success(value) =>
        value

  "Data" should "load properly" in:
    checkByQuery(List(List(53.0)), "SELECT COUNT(*) FROM `2te116u_tmy_test_simple_rules`")
    checkByQuery(List(List(150.0)), "SELECT COUNT(*) FROM `ivolga_test_narrow`")
    checkByQuery(List(List(48.0)), "SELECT COUNT(*) FROM `ivolga_test_wide`")
    checkByQuery(List(List(159.0)), "SELECT COUNT(*) FROM math_test")

  "Cases 1-17, 43-53" should "work in wide table" in:
    casesPatterns.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(s"17wide_$id", wideInputConf, Seq(wideOutputConf), 50, List(casesPatterns(id)))
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
    }
    Thread.sleep(15000)
    alertByQuery(
      incidentsCount
        .map { case (k, v) =>
          List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_test"))

  "Cases 1-17, 43-53" should "work in narrow table" in:
    casesPatterns.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(s"17narrow_$id", narrowInputConf, Seq(narrowOutputConf), 50, List(casesPatterns(id)))
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
    }
    Thread.sleep(15000)
    alertByQuery(
      incidentsCount
        .map { case (k, v) =>
          List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_narrow_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_narrow_test"))

  "Cases 18-42" should "work in ivolga wide table" in:
    casesPatternsIvolga.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(
          s"17wide_$id",
          wideInputIvolgaConf,
          Seq(wideOutputIvolgaConf),
          50,
          List(casesPatternsIvolga(id))
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
    }
    Thread.sleep(15000)
    alertByQuery(
      incidentsIvolgaCount
        .map { case (k, v) =>
          List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery(
        "events_wide_ivolga_test",
        numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted)
      )
    )
    alertByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_wide_ivolga_test"))

  "Cases 18-42" should "work in ivolga narrow table" in:
    casesPatternsIvolga.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(
          s"17narrow_$id",
          narrowInputIvolgaConf,
          Seq(narrowOutputIvolgaConf),
          50,
          List(casesPatternsIvolga(id))
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
    }
    Thread.sleep(15000)
    alertByQuery(
      incidentsIvolgaCount
        .map { case (k, v) =>
          List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery(
        "events_narrow_ivolga_test",
        numbersToRanges(casesPatternsIvolga.keys.map(_.toInt).toList.sorted)
      )
    )
    alertByQuery(incidentsIvolgaTimestamps, secondValidationQuery.format("events_narrow_ivolga_test"))

  def numbersToRanges(numbers: List[Int]): List[Range] =
    @tailrec
    def inner(in: List[Int], acc: List[Range]): List[Range] = (in, acc) match
      case (Nil, a)                               => a.reverse
      case (n :: tail, r :: tt) if n == r.end + 1 => inner(tail, (r.start to n) :: tt)
      case (n :: tail, a)                         => inner(tail, (n to n) :: a)

    inner(numbers, Nil)

  "Cases 1-17, 43-50" should "work in wide Kafka table" in:
    // use MemoryCheckpointing for Kafka tests
    CheckpointingService.forceCreate(None)
    casesPatterns.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(
          s"17kafkawide_$id",
          wideKafkaInputConf,
          Seq(wideKafkaOutputConf),
          50,
          List(casesPatterns(id))
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
        // alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
    }
    Thread.sleep(15000)
    casesPatterns.keys.foreach { id =>
      Get(s"/job/17kafkawide_$id/stop") ~> route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
    }
    alertByQuery(
      incidentsCount
        .map { case (k, v) =>
          List(k.toDouble, v.toDouble)
        }
        .toList
        .sortBy(_.headOption.getOrElse(Double.NaN)),
      firstValidationQuery("events_wide_kafka_test", numbersToRanges(casesPatterns.keys.map(_.toInt).toList.sorted))
    )
    alertByQuery(incidentsTimestamps, secondValidationQuery.format("events_wide_kafka_test"))

  "Cases 1-17, 43-50" should "work in wide Kafka sink" in:
    casesPatterns.keys.foreach { id =>
      Post(
        submitUrl,
        FindPatternsRequest(
          s"17tokafkawide_$id",
          wideInputConf,
          Seq(wideToKafkaOutputConf),
          50,
          List(casesPatterns(id))
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
        // alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
    }
    Thread.sleep(15000)
    // TODO: Verify results

  "Cases 1-17, 43-50" should "be validated" in:
    casesPatterns.keys.foreach { id =>
      Post(
        "/patterns/validate/",
        PatternsValidatorConf(
          List(casesPatterns(id)),
          Map()
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
        // alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
    }

  "Cases 18-42" should "be validated" in:
    casesPatternsIvolga.keys.foreach { id =>
      Post(
        "/patterns/validate/",
        PatternsValidatorConf(
          List(casesPatternsIvolga(id)),
          Map()
        )
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: $id"):
          status shouldEqual StatusCodes.OK
        // alertByQuery(List(List(id.toDouble, incidentsCount(id).toDouble)), s"SELECT $id, COUNT(*) FROM events_wide_test WHERE id = $id")
    }

  "Cases" should "not work in wrong source" in:
    Post(
      submitUrl,
      FindPatternsRequest(
        s"17widewrongsource_1",
        wideInputConf.copy(jdbcUrl = wrongJdbcUrl),
        Seq(wideOutputConf),
        50,
        casesPatterns.values.toList
      )
    ) ~>
    route ~> check:
      status shouldEqual StatusCodes.BadRequest
      entityAs[FailureResponse].errorCode shouldBe 4030

  // "Cases" should "not work in wrong sink" in {
  //   Post(
  //     submitUrl,
  //     FindPatternsRequest(
  //       s"17widewrongsink_1",
  //       wideInputConf,
  //       Seq(wideOutputConf.copy(jdbcUrl = wrongJdbcUrl)),
  //       50,
  //       casesPatterns.values.toList
  //     )
  //   ) ~>
  //   route ~> check {
  //     status shouldEqual StatusCodes.BadRequest
  //     entityAs[FailureResponse].errorCode shouldBe 4040
  //   }
  // }

  "Wrong patterns" should "not be launched" in:
    val wrongPatterns = Seq(
      RawPattern(id = -1, sourceCode = "POilDieselOut += 9.1"),
      RawPattern(id = -2, sourceCode = "POilDieselOut == 9.22"),
      RawPattern(id = -3, sourceCode = "POilDieselOut = 9.0 for"),
      RawPattern(id = -4, sourceCode = "POilDieselOut = 9.0 for 20"),
      RawPattern(id = -5, sourceCode = "POilDieselOut = 10.0 for 5 minut"),
      RawPattern(id = -6, sourceCode = "wait(POilDieselOut = 9.0)"),
      RawPattern(id = -7, sourceCode = "POilDieselOut = 9.0 for andThen"),
      RawPattern(id = -8, sourceCode = "PowerPolling for 42")
    )
    wrongPatterns.foreach { pat =>
      Post(
        submitUrl,
        FindPatternsRequest(s"17wrong_${pat.id}", wideInputConf, Seq(wideOutputConf), 50, List(pat))
      ) ~>
      route ~> check:
        withClue(s"Pattern ID: ${pat.id}"):
          // status shouldEqual StatusCodes.BadRequest
          entityAs[FailureResponse].errorCode shouldBe 4020
    }
