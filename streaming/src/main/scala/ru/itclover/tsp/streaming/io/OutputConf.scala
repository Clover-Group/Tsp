package ru.itclover.tsp.streaming.io

import cats.effect.{IO, MonadCancelThrow, Resource}
import cats.implicits._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
//import doobie.WeakAsync.doobieWeakAsyncForAsync
import doobie.{ConnectionIO, Transactor}
import doobie.implicits.toSqlInterpolator
import doobie.implicits.javatimedrivernative.JavaLocalDateTimeMeta
import doobie.util.fragment.Fragment
import fs2.Pipe
import fs2.kafka.{Acks, KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer}
import ru.itclover.tsp.StreamSource.Row

import java.sql.{Connection, Timestamp}
import java.time.{ZoneId, ZonedDateTime}
import java.sql.Types
import java.time.format.DateTimeFormatter
import doobie.util.log.LogHandler
import doobie.util.log.LogEvent
import com.typesafe.scalalogging.Logger
import java.sql.JDBCType
import doobie.util.meta.Meta
import doobie.util.meta.TimeMetaInstances
import java.time.ZoneOffset
import ru.itclover.tsp.core.Time
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import doobie.util.Put.Basic
import doobie.util.Write
import java.time.LocalDateTime

trait OutputConf[Event] {

  def getSink: Pipe[IO, fs2.Chunk[Event], Unit]

  def parallelism: Option[Int]

  def rowSchema: EventSchema

  def batchSize: Int
}

/** Sink for anything that support JDBC connection
  * @param rowSchema
  *   schema of writing rows
  * @param jdbcUrl
  *   example - "jdbc:clickhouse://localhost:8123/default?"
  * @param driverName
  *   example - "com.clickhouse.jdbc.ClickHouseDriver"
  * @param userName
  *   for JDBC auth
  * @param password
  *   for JDBC auth
  * @param batchInterval
  *   batch size for writing found incidents
  * @param parallelism
  *   num of parallel task to write data
  */
case class JDBCOutputConf(
  tableName: String,
  rowSchema: EventSchema,
  jdbcUrl: String,
  driverName: String,
  password: Option[String] = None,
  batchInterval: Option[Int] = None,
  userName: Option[String] = None,
  parallelism: Option[Int] = Some(1)
) extends OutputConf[Row] {

  def fixedDriverName: String = driverName match {
    case "ru.yandex.clickhouse.ClickHouseDriver" => "com.clickhouse.jdbc.ClickHouseDriver"
    case _                                       => driverName
  }

  override def getSink: Pipe[IO, fs2.Chunk[Row], Unit] =
    source => fuseMap(source, insertQuery)(transactor).drain

  lazy val (queryUserName, queryPassword) = getCreds

  lazy val log = Logger[JDBCOutputConf]

  lazy val logHandler = new LogHandler[IO] {

    override def run(logEvent: LogEvent): IO[Unit] = IO { log.debug(logEvent.sql) }

  }

  implicit val datetimeMeta: Meta[ZonedDateTime] =
    JavaLocalDateTimeMeta.timap(ldt => ldt.atZone(ZoneId.of("UTC")))(zdt => zdt.toLocalDateTime())

  lazy val transactor = Transactor.fromDriverManager[IO](
    fixedDriverName,
    jdbcUrl,
    userName.getOrElse(queryUserName),
    password.getOrElse(queryPassword),
    Some(logHandler)
  )

  def getCreds: (String, String) = {
    try {
      val query = jdbcUrl.split("\\?", 2).lift(1).getOrElse("")
      val params = query
        .split("&")
        .map(kv => kv.split("=", 2))
        .map {
          case Array(k)        => (k, "")
          case Array(k, v)     => (k, v)
          case Array(k, v, _*) => (k, v)
        }
        .toMap
      (params.getOrElse("user", ""), params.getOrElse("password", ""))
    } catch {
      case e: Exception =>
        ("", "")
    }
  }

  def insertQuery(data: fs2.Chunk[Row]): ConnectionIO[Int] = {
    val fields = rowSchema.fieldsNames.mkString(", ")
    (
      fr"""insert into """
        ++ Fragment.const(s"$tableName ($fields)")
        ++ fr"values"
        ++ data.asSeq.toList
          .map(r =>
            fr"(" ++ r.toList
              .zip(rowSchema.fieldsTypes)
              .map((x, t) => fragmentForDataValue(x, t))
              .intercalate(fr",") ++ fr")"
          )
          .intercalate(fr", ")
    ).update.run
  }

  def createTableQuery: ConnectionIO[Int] = {
    val fieldsWithTypes = rowSchema.fieldsNames
      .zip(rowSchema.fieldsTypes)
      .map { case (f, t) =>
        Fragment.const(s"$f ${JDBCType.valueOf(t).getName()}")
      }
      .intercalate(fr",")
    var fragment =
      fr"create table if not exists"
        ++ Fragment.const(s"$tableName")
        ++ fr"(" ++ fieldsWithTypes ++ fr")"
    if (driverName.toLowerCase().contains("clickhouse")) {
      fragment ++= fr"engine = Log()"
    }
    fragment.update.run
  }

  val timeFormatterParser = new DateTimeFormatterBuilder()
    .appendPattern("uuuu-MM-dd HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .toFormatter()
    .withZone(ZoneId.of("UTC"))

  def fragmentForDataValue(x: Object, t: Int): Fragment = {
    t match {
      case Types.INTEGER => fr"${x.toString.toInt}"
      case Types.FLOAT   => fr"${x.toString.toFloat}"
      case Types.DOUBLE  => fr"${x.toString.toDouble}"
      case Types.TIMESTAMP =>
        fr"${ZonedDateTime.parse(x.toString, timeFormatterParser)}"
      case _ => fr"${x.toString}"
    }
  }

  def fuseMap[A, B](
    source: fs2.Stream[IO, A],
    sink: A => ConnectionIO[B]
  )(
    sinkXA: Transactor[IO]
  ): fs2.Stream[IO, B] =
    fuseMapGeneric(source, identity[A], sink)(sinkXA)

  def fuseMapGeneric[F[_], A, B, C](
    source: fs2.Stream[IO, A],
    sourceToSink: A => B,
    sink: B => ConnectionIO[C]
  )(
    sinkTransactor: Transactor[F]
  )(implicit
    ev: MonadCancelThrow[F]
  ): fs2.Stream[F, C] = {

    // Interpret a ConnectionIO into a Kleisli arrow for F via the sink interpreter.
    def interpS[T](f: ConnectionIO[T]): Connection => F[T] =
      f.foldMap(sinkTransactor.interpret).run

    // Open a connection in `F` via the sink transactor. Need patmat due to the existential.
    val conn: Resource[F, Connection] =
      sinkTransactor match { case xa => xa.connect(xa.kernel) }

    // Given a Connection we can construct the stream we want.
    def mkStream(c: Connection): fs2.Stream[F, C] = {

      // Now we can interpret a ConnectionIO into a Stream of F via the sink interpreter.
      def evalS(f: ConnectionIO[_]): fs2.Stream[F, Nothing] =
        fs2.Stream.eval(interpS(f)(c)).drain

      // And can thus lift all the sink operations into Stream of F
      val sinkEval: A => fs2.Stream[F, C] = (a: A) => evalS(sink(sourceToSink(a)))
      val create = evalS(createTableQuery)
      val before = evalS(sinkTransactor.strategy.before)
      val after = evalS(sinkTransactor.strategy.after)

      // And construct our final stream.
      create ++ before ++ source.flatMap(sinkEval).asInstanceOf[fs2.Stream[F, C]] ++ after
      // source.flatMap(sinkEval).asInstanceOf[fs2.Stream[F, C]]
    }

    // And we're done!
    fs2.Stream.resource(conn).flatMap(mkStream)

  }

  override def batchSize: Int = batchInterval.getOrElse(100)
}

///**
//  * "Empty" sink (for example, used if one need only to report timings)
//  */
//case class EmptyOutputConf() extends OutputConf[Row] {
//  override def forwardedFieldsIds: Seq[String] = Seq()
//  override def getOutputFormat: OutputFormat[Row] = ???
//  override def parallelism: Option[Int] = Some(1)
//}

/** Sink for kafka connection
  * @param broker
  *   host and port for kafka broker
  * @param topic
  *   where is data located
  * @param serializer
  *   format of data in kafka
  * @param rowSchema
  *   schema of writing rows
  * @param parallelism
  *   num of parallel task to write data
  * @author
  *   trolley813
  */
case class KafkaOutputConf(
  broker: String,
  topic: String,
  serializer: Option[String] = Some("json"),
  rowSchema: EventSchema,
  parallelism: Option[Int] = Some(1)
) extends OutputConf[Row] {

  val producerSettings = ProducerSettings(
    keySerializer = Serializer[IO, String],
    valueSerializer = Serializer[IO, String]
  ).withBootstrapServers(broker)
    .withAcks(Acks.All)
    .withProperty("auto.create.topics.enable", "true")

  override def getSink: Pipe[IO, fs2.Chunk[Row], Unit] = stream =>
    stream
      .map { data =>
        val recs = data.asSeq.map { row =>
          val serialized = serialize(row, rowSchema)
          ProducerRecord(topic, ZonedDateTime.now(ZoneId.of("UTC")).toString, serialized)
        }
        fs2.Chunk(recs: _*)
      }
      .through(KafkaProducer.pipe(producerSettings))
      .drain

  def serialize(output: Row, eventSchema: EventSchema): String = {

    val mapper = new ObjectMapper()
    val root = mapper.createObjectNode()

    // TODO: Write JSON

    eventSchema match {
      case newRowSchema: NewRowSchema =>
        newRowSchema.data.foreach { case (k, v) =>
          putValueToObjectNode(k, v, root, output(newRowSchema.fieldsIndices(String(k))))
        }
    }

    def putValueToObjectNode(k: String, v: EventSchemaValue, root: ObjectNode, value: Object): Unit = {
      v.`type` match {
        case "int8"    => root.put(k, value.asInstanceOf[Byte])
        case "int16"   => root.put(k, value.asInstanceOf[Short])
        case "int32"   => root.put(k, value.asInstanceOf[Int])
        case "int64"   => root.put(k, value.asInstanceOf[Long])
        case "float32" => root.put(k, value.asInstanceOf[Float])
        case "float64" => root.put(k, value.asInstanceOf[Double])
        case "boolean" => root.put(k, value.asInstanceOf[Boolean])
        case "string"  => root.put(k, value.asInstanceOf[String])
        case "timestamp" =>
          root.put(
            k,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME
              .withZone(ZoneId.of("UTC"))
              .format(value.asInstanceOf[Timestamp].toInstant())
          )
        case "object" =>
          val data = value.toString
          val parsedJson = mapper.readTree(data)
          root.put(k, parsedJson)
      }
    }

    mapper.writeValueAsString(root)

  }

  override def batchSize: Int = 100
}
