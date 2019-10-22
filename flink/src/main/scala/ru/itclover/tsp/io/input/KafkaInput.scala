package ru.itclover.tsp.io.input

import java.util.UUID
import org.apache.flink.types.Row

@SerialVersionUID(91000L)
case class KafkaInputConf(
  brokers: String,
  topic: String,
  group: String = UUID.randomUUID().toString,
  datetimeField: Symbol,
  partitionFields: Seq[Symbol],
  dataTransformation: Option[SourceDataTransformation[Row, Symbol, Any]] = None,
  timestampMultiplier: Option[Double] = Some(1000.0),
  fieldsTypes: Map[String, String],
) extends InputConf[Row, Symbol, Any] {

  def chunkSizeMs: Option[Long] = Some(10L)
  def defaultEventsGapMs: Long = 0L
  def defaultToleranceFraction: Option[Double] = Some(0.1)
  def eventsMaxGapMs: Long = 1L
  def numParallelSources: Option[Int] = Some(1)
  def parallelism: Option[Int] = Some(1)
  def patternsParallelism: Option[Int] = Some(1)
  def sourceId: Int = 1

}
