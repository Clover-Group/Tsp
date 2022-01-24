package ru.itclover.tsp.http

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ru.itclover.tsp.http.routes.JobReporting
//import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec
import ru.itclover.tsp.http.domain.input.FindPatternsRequest
import ru.itclover.tsp.http.utils.SqlMatchers
import ru.itclover.tsp.io.input.JDBCInputConf
import ru.itclover.tsp.io.output.{JDBCOutputConf, NewRowSchema}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

class NonExistentBaseTest extends FlatSpec with SqlMatchers with ScalatestRouteTest with HttpService {
  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment()

  // to run blocking tasks.
  val blockingExecutorContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(
        0, // corePoolSize
        Int.MaxValue, // maxPoolSize
        1000L, //keepAliveTime
        TimeUnit.MILLISECONDS, //timeUnit
        new SynchronousQueue[Runnable]() //workQueue
        //new ThreadFactoryBuilder().setNameFormat("blocking-thread").setDaemon(true).build()
      )
    )

  val dummyPort = 6000

  val inputConf = JDBCInputConf(
    sourceId = 123,
    jdbcUrl = s"jdbc:clickhouse://localhost:$dummyPort/default",
    query = """select *, speed as "speed(1)(2)" from Test.SM_basic_wide""", // speed(1)(2) fancy colnames test
    driverName = "ru.yandex.clickhouse.ClickHouseDriver",
    datetimeField = 'datetime,
    eventsMaxGapMs = Some(60000L),
    defaultEventsGapMs = Some(1000L),
    chunkSizeMs = Some(900000L),
    partitionFields = Seq('series_id, 'mechanism_id)
  )

  val rowSchema = NewRowSchema('series_storage, 'from, 'to, ('app, 1), 'id, 'subunit, 'uuid)

  val outputConf = JDBCOutputConf(
    "Test.SM_basic_patterns",
    rowSchema,
    s"jdbc:clickhouse://localhost:$dummyPort/default",
    "ru.yandex.clickhouse.ClickHouseDriver"
  )

  "Non-existent database" should "give error upon execution" in {
    Post("/streamJob/from-jdbc/to-jdbc/?run_async=0", FindPatternsRequest("1", inputConf, outputConf, 50, Seq())) ~>
    route ~> check {
      status shouldEqual StatusCodes.BadRequest
    }
  }
  override val reporting: Option[JobReporting] = None
}
