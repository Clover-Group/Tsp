package ru.itclover.tsp.http.utils

import com.dimafeng.testcontainers.SingleContainer
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import org.testcontainers.containers.wait.strategy.WaitStrategy
import org.testcontainers.containers.{BindMode, GenericContainer => OTCGenericContainer}
import ru.itclover.tsp.services.InfluxDBService

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.{Failure, Success}

// Default arguments for constructing a container are useful
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
class InfluxDBContainer(
                         imageName: String,
                         val portsBindings: List[(Int, Int)] = List.empty,
                         val url: String,
                         val dbName: String,
                         val userName: String,
                         val password: String = "",
                         env: Map[String, String] = Map(),
                         command: Seq[String] = Seq(),
                         classpathResourceMapping: Seq[(String, String, BindMode)] = Seq(),
                         waitStrategy: Option[WaitStrategy] = None
                       ) extends SingleContainer[OTCGenericContainer[_]] {

  type OTCContainer = OTCGenericContainer[T] forSome { type T <: OTCGenericContainer[T] }
  implicit override val container: OTCContainer = new OTCGenericContainer(imageName)

  if (portsBindings.nonEmpty) {
    val bindings = portsBindings.map { case (out, in) => s"${out.toString}:${in.toString}" }
    val _ = container.setPortBindings(bindings.asJava)
  }
  env.foreach(Function.tupled(container.withEnv))
  if (command.nonEmpty) {
    val _ = container.withCommand(command: _*)
  }
  classpathResourceMapping.foreach(Function.tupled(container.withClasspathResourceMapping))
  waitStrategy.foreach(container.waitingFor)

  // We cannot initialise `db` here yet
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  var db: InfluxDB = _

  // This must throw an exception upon error
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  override def start(): Unit = {
    super.start()
    val conf = InfluxDBService.InfluxConf(url, dbName, Some(userName), Some(password), 30L, false)
    db = InfluxDBService.connectDb(conf) match {
      case Success(database)  => database
      case Failure(exception) => throw exception
    }
  }

  override def stop(): Unit = {
    super.stop()
    db.close()
  }

  def executeQuery(sql: String): QueryResult = db.query(new Query(sql, dbName))

  def executeUpdate(sql: String): Unit = db.write(sql)
}
