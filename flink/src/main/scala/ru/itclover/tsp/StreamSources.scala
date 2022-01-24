package ru.itclover.tsp

import cats.syntax.either._
import com.typesafe.scalalogging.Logger
import org.apache.flink.api.common.io.RichInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.types.Row
import org.influxdb.dto.QueryResult
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor}
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.io.input._
import ru.itclover.tsp.io.{EventCreator, EventCreatorInstances}
import ru.itclover.tsp.services.{InfluxDBService, JdbcService, KafkaService}
import ru.itclover.tsp.transformers.SparseRowsDataAccumulator
import ru.itclover.tsp.utils.ErrorsADT._
import ru.itclover.tsp.utils.RowOps.{RowIsoTimeExtractor, RowSymbolExtractor, RowTsTimeExtractor}
import ru.itclover.tsp.utils.{KeyCreator, KeyCreatorInstances}

import scala.collection.JavaConverters._
import scala.collection.mutable

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
trait StreamSource[Event, EKey, EItem] extends Product with Serializable {
  def createStream: DataStream[Event]

  def conf: InputConf[Event, EKey, EItem]

  def fieldsClasses: Seq[(Symbol, Class[_])]

  def transformedFieldsClasses: Seq[(Symbol, Class[_])] = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, mapping, _)) =>
      val m: Map[EKey, List[EKey]] = mapping.getOrElse(Map.empty)
      val r = fieldsClasses ++ m.map {
          case (col, list) =>
            list.map(
              k =>
                (
                  eKeyToField(k),
                  fieldsClasses
                    .find {
                      case (s, _) => fieldToEKey(s) == col
                    }
                    .map(_._2)
                    .getOrElse(defaultClass)
                )
            )
        }.flatten
      r
    case _ =>
      fieldsClasses
  }

  def defaultClass: Class[_] = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(_, value, _, _, _)) =>
      fieldsClasses.find { case (s, _) => fieldToEKey(s) == value }.map(_._2).getOrElse(classOf[Double])
    case _ =>
      classOf[Double]
  }

  def fieldToEKey: Symbol => EKey

  def eKeyToField: EKey => Symbol

  def fieldsIdxMap: Map[Symbol, Int]

  def transformedFieldsIdxMap: Map[Symbol, Int]

  def partitioner: Event => String

  def transformedPartitioner: Event => String

  implicit def timeExtractor: TimeExtractor[Event]

  implicit def transformedTimeExtractor: TimeExtractor[Event]

  implicit def idxExtractor: IdxExtractor[Event]

  implicit def extractor: Extractor[Event, EKey, EItem]

  implicit def transformedExtractor: Extractor[Event, EKey, EItem]

  implicit def trivialEItemDecoder: Decoder[EItem, EItem] = (v1: EItem) => v1

  implicit def itemToKeyDecoder: Decoder[EItem, EKey] // for narrow data widening

  implicit def kvExtractor: Event => (EKey, EItem) = conf.dataTransformation match {
    case Some(NarrowDataUnfolding(key, value, _, mapping, _)) =>
      (r: Event) =>
        // TODO: Maybe optimise that by using intermediate (non-serialised) dictionary
        val extractedKey = extractor.apply[EKey](r, key)
        val valueColumn = mapping
          .getOrElse(Map.empty[EKey, List[EKey]])
          .toSeq
          .find {
            case (_, list) =>
              list.contains(extractedKey)
          }
          .map(_._1)
          .getOrElse(value)
        val extractedValue = extractor.apply[EItem](r, valueColumn)
        (extractedKey, extractedValue) // TODO: See that place better
    case Some(WideDataFilling(_, _)) =>
      (_: Event) => sys.error("Wide data filling does not need K-V extractor")
    case Some(_) =>
      (_: Event) => sys.error("Unsupported data transformation")
    case None =>
      (_: Event) => sys.error("No K-V extractor without data transformation")
  }

  implicit def eventCreator: EventCreator[Event, EKey]

  implicit def keyCreator: KeyCreator[EKey]

  def patternFields: Set[EKey]
}

object StreamSource {

  def findNullField(allFields: Seq[Symbol], excludedFields: Seq[Symbol]) =
    allFields.find { field =>
      !excludedFields.contains(field)
    }
}

case class RowWithIdx(idx: Idx, row: Row)

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object JdbcSource {

  def create(conf: JDBCInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[Err, JdbcSource] =
    for {
      types <- JdbcService
        .fetchFieldsTypesInfo(conf.driverName, conf.jdbcUrl, conf.query)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      newFields <- checkKeysExistence(conf, fields)
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => JdbcSource(conf, types, nullField, newFields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source

  def checkKeysExistence(conf: JDBCInputConf, keys: Set[Symbol]): Either[GenericRuntimeErr, Set[Symbol]] =
    conf.dataTransformation match {
      case Some(NarrowDataUnfolding(keyColumn, _, _, _, _)) =>
        JdbcService
          .fetchAvailableKeys(conf.driverName, conf.jdbcUrl, conf.query, keyColumn)
          .toEither
          .map(_.intersect(keys))
          .leftMap[GenericRuntimeErr](e => GenericRuntimeErr(e, 5099))
      case _ => Right(keys)
    }
}

// todo rm nullField and trailing nulls in queries at platform (uniting now done on Flink) after states fix
// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class JdbcSource(
  conf: JDBCInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[RowWithIdx, Symbol, Any] {

  import conf._

  val stageName = "JDBC input processing stage"
  val log = Logger[JdbcSource]
  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  def partitionsIdx = partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = partitionFields.map(transformedFieldsIdxMap)

  require(fieldsIdxMap.get(datetimeField).isDefined, "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(datetimeField)
  val fieldsTypesInfo: Array[TypeInformation[_]] = fieldsClasses.map(c => TypeInformation.of(c._2)).toArray
  val rowTypesInfo = new RowTypeInfo(fieldsTypesInfo, fieldsClasses.map(_._1.toString.tail).toArray)

  override def createStream = {

    val stream = streamEnv.createInput(inputFormat).name(stageName)

    val ascendingExtractor = new AscendingTimestampExtractor[RowWithIdx] {
      override def extractAscendingTimestamp(element: RowWithIdx): Long = timeExtractor(element).toMillis
    }

    parallelism
      .fold(stream)(stream.setParallelism)
      .map(r => RowWithIdx(0, r)) // todo hack look in PatternProcessor for actual idx
      .assignTimestampsAndWatermarks(ascendingExtractor)
  }

  override def fieldToEKey = { fieldId: Symbol =>
    fieldId
  // fieldsIdxMap(fieldId)
  }

  override def eKeyToField: Symbol => Symbol = { key: Symbol =>
    key
  }

  override def partitioner: RowWithIdx => String = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  override def transformedPartitioner: RowWithIdx => String = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  def tsMultiplier = timestampMultiplier.getOrElse {
    log.trace("timestampMultiplier in JDBC source conf is not provided, use default = 1000.0")
    1000.0
  }

  override def timeExtractor: TimeExtractor[RowWithIdx] = {
    val rowExtractor = RowTsTimeExtractor(timeIndex, tsMultiplier, datetimeField)
    TimeExtractor.of(r => rowExtractor(r.row))
  }
  override def extractor = RowSymbolExtractor(fieldsIdxMap).comap(_.row)

  override def transformedExtractor = RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  // JDBC uses Java's null, so we must use it for interop
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val inputFormat: RichInputFormat[Row, InputSplit] =
    JDBCInputFormatProps
      .buildJDBCInputFormat()
      .setDrivername(driverName)
      .setDBUrl(jdbcUrl)
      .setUsername(userName.orNull)
      .setPassword(password.orNull)
      .setQuery(query)
      .setRowTypeInfo(rowTypesInfo)
      .finish()

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[RowWithIdx, Symbol, Any, RowWithIdx](this, patternFields)(
        createTypeInformation[RowWithIdx],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, datetimeField).comap(_.row)

  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)
}

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object InfluxDBSource {

  def create(conf: InfluxDBInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, InfluxDBSource] =
    for {
      types <- InfluxDBService
        .fetchFieldsTypesInfo(conf.query, conf.influxConf)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => InfluxDBSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source
}

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class InfluxDBSource(
  conf: InfluxDBInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[RowWithIdx, Symbol, Any] {

  import conf._

  // we use cast for Java class, so we need to use asInstanceOf
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  val dummyResult: Class[QueryResult.Result] = new QueryResult.Result().getClass.asInstanceOf[Class[QueryResult.Result]]
  val queryResultTypeInfo: TypeInformation[QueryResult.Result] = TypeInformation.of(dummyResult)
  val stageName = "InfluxDB input processing stage"
  val defaultTimeoutSec = 200L

  val fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  val fieldsIdxMap = fieldsIdx.toMap
  def partitionsIdx = partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = partitionFields.map(transformedFieldsIdxMap)

  require(fieldsIdxMap.contains(datetimeField), "Cannot find datetime field, index overflow.")
  require(fieldsIdxMap(datetimeField) < fieldsIdxMap.size, "Cannot find datetime field, index overflow.")
  private val badPartitions = partitionFields
    .map(fieldsIdxMap.get)
    .find(idx => idx.getOrElse(Int.MaxValue) >= fieldsIdxMap.size)
    .flatten
    .map(p => fieldsClasses(p)._1)
  require(badPartitions.isEmpty, s"Cannot find partition field (${badPartitions.getOrElse('unknown)}), index overflow.")

  val timeIndex = fieldsIdxMap(datetimeField)
  val transformedTimeIndex = transformedFieldsIdxMap(datetimeField)
  val fieldsTypesInfo: Array[TypeInformation[_]] = fieldsClasses.map(c => TypeInformation.of(c._2)).toArray
  val rowTypesInfo = new RowTypeInfo(fieldsTypesInfo, fieldsClasses.map(_._1.toString.tail).toArray)

  override def createStream = {
    val serFieldsIdxMap = fieldsIdxMap // for task serialization
    val stream = streamEnv
      .createInput(inputFormat)(queryResultTypeInfo)
      .flatMap(queryResult => {
        // extract Flink.rows form series of points
        if (queryResult == null || queryResult.getSeries == null) {
          mutable.Buffer[Row]()
        } else
          for {
            series   <- queryResult.getSeries.asScala
            valueSet <- series.getValues.asScala.map(_.asScala)
            if valueSet != null
          } yield {
            val tags = if (series.getTags != null) series.getTags.asScala else Map.empty
            val row = new Row(tags.size + valueSet.size)
            val fieldsAndValues = tags ++ series.getColumns.asScala.zip(valueSet)
            fieldsAndValues.foreach {
              case (field, value) => row.setField(serFieldsIdxMap(Symbol(field)), value)
            }
            row
          }
      })
      .name(stageName)
    (parallelism match {
      case Some(p) => stream.setParallelism(p)
      case None    => stream
    }).map(r => RowWithIdx(0, r)) // todo hack look in PatternProcessor for actual idx
  }

  override def fieldToEKey = (fieldId: Symbol) => fieldId // fieldsIdxMap(fieldId)

  override def eKeyToField: Symbol => Symbol = (key: Symbol) => key

  override def partitioner = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  override def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  override def timeExtractor = RowIsoTimeExtractor(timeIndex, datetimeField).comap(_.row)
  override def extractor = RowSymbolExtractor(fieldsIdxMap).comap(_.row)
  override def transformedExtractor = RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  val inputFormat =
    InfluxDBInputFormat
      .create()
      .url(url)
      .timeoutSec(timeoutSec.getOrElse(defaultTimeoutSec))
      .username(userName.getOrElse(""))
      .password(password.getOrElse(""))
      .database(dbName)
      .query(query)
      .and()
      .buildIt()

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[RowWithIdx, Symbol, Any, RowWithIdx](this, patternFields)(
        createTypeInformation[RowWithIdx],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowIsoTimeExtractor(transformedTimeIndex, datetimeField).comap(_.row)

  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)
}

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object KafkaSource {

  val log = Logger[KafkaSource]

  def create(conf: KafkaInputConf, fields: Set[Symbol])(
    implicit strEnv: StreamExecutionEnvironment
  ): Either[ConfigErr, KafkaSource] =
    for {
      types <- KafkaService
        .fetchFieldsTypesInfo(conf)
        .toEither
        .leftMap[ConfigErr](e => SourceUnavailable(Option(e.getMessage).getOrElse(e.toString)))
      _ = log.info(s"Kafka types found: $types")
      source <- StreamSource.findNullField(types.map(_._1), conf.datetimeField +: conf.partitionFields) match {
        case Some(nullField) => KafkaSource(conf, types, nullField, fields).asRight
        case None            => InvalidRequest("Source should contain at least one non partition and datatime field.").asLeft
      }
    } yield source

}

// Fields types are only known at runtime, so we have to use Any here
@SuppressWarnings(Array("org.wartremover.warts.Any"))
case class KafkaSource(
  conf: KafkaInputConf,
  fieldsClasses: Seq[(Symbol, Class[_])],
  nullFieldId: Symbol,
  patternFields: Set[Symbol]
)(
  implicit @transient streamEnv: StreamExecutionEnvironment
) extends StreamSource[RowWithIdx, Symbol, Any] {

  val log = Logger[KafkaSource]

  def fieldsIdx = fieldsClasses.map(_._1).zipWithIndex
  def fieldsIdxMap = fieldsIdx.toMap

  override def fieldToEKey: Symbol => Symbol = (x => x)

  override def eKeyToField: Symbol => Symbol = (key: Symbol) => key

  def timeIndex = fieldsIdxMap(conf.datetimeField)

  def tsMultiplier = conf.timestampMultiplier.getOrElse {
    log.debug("timestampMultiplier in Kafka source conf is not provided, use default = 1000.0")
    1000.0
  }

  implicit def extractor: ru.itclover.tsp.core.io.Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(fieldsIdxMap).comap(_.row)
  implicit def timeExtractor: ru.itclover.tsp.core.io.TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(timeIndex, tsMultiplier, conf.datetimeField).comap(_.row)

  val stageName = "Kafka input processing stage"

  def createStream: DataStream[RowWithIdx] = {
    // TODO: Make this parameter configurable
    val consumer = KafkaService.consumer(conf, fieldsIdxMap).setStartFromGroupOffsets()
    streamEnv
      .enableCheckpointing(5000)
      .addSource(consumer)
      // we need to set parallelism of Kafka source to 1 to persist ordering.
      // If we know number of topic partitions, it's better to pass this number here.
      .setParallelism(conf.numParallelSources.getOrElse(1))
      .name(stageName)
    //.keyBy(_ => "nokey")
    //.process(new TimeOutFunction(5000, timeIndex, fieldsIdxMap.size))
  }.map(r => RowWithIdx(0, r)) // todo hack look in PatternProcessor for actual idx

  def partitionsIdx = conf.partitionFields.filter(fieldsIdxMap.contains).map(fieldsIdxMap)
  def transformedPartitionsIdx = conf.partitionFields.map(transformedFieldsIdxMap)

  def partitioner = {
    val serializablePI = partitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  def transformedPartitioner = {
    val serializablePI = transformedPartitionsIdx
    event: RowWithIdx => serializablePI.map(event.row.getField).mkString
  }

  override def transformedFieldsIdxMap: Map[Symbol, Int] = conf.dataTransformation match {
    case Some(_) =>
      val acc = SparseRowsDataAccumulator[RowWithIdx, Symbol, Any, RowWithIdx](this, patternFields)(
        createTypeInformation[RowWithIdx],
        timeExtractor,
        kvExtractor,
        extractor,
        eventCreator,
        keyCreator
      )
      acc.allFieldsIndexesMap
    case None =>
      fieldsIdxMap
  }

  val transformedTimeIndex = transformedFieldsIdxMap(conf.datetimeField)

  implicit override def transformedTimeExtractor: TimeExtractor[RowWithIdx] =
    RowTsTimeExtractor(transformedTimeIndex, tsMultiplier, conf.datetimeField).comap(_.row)

  implicit override def transformedExtractor: Extractor[RowWithIdx, Symbol, Any] =
    RowSymbolExtractor(transformedFieldsIdxMap).comap(_.row)

  implicit override def itemToKeyDecoder: Decoder[Any, Symbol] = (x: Any) => Symbol(x.toString)

  implicit override def eventCreator: EventCreator[RowWithIdx, Symbol] =
    EventCreatorInstances.rowWithIdxSymbolEventCreator

  implicit override def keyCreator: KeyCreator[Symbol] = KeyCreatorInstances.symbolKeyCreator
  //todo refactor everything related to idxExtractor
  implicit override def idxExtractor: IdxExtractor[RowWithIdx] = IdxExtractor.of(_.idx)

}
