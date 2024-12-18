package ru.itclover.tsp.streaming.utils

import ru.itclover.tsp.StreamSource.Row

import java.time.Instant
import ru.itclover.tsp.core.io.{Decoder, Extractor, TimeExtractor}
import ru.itclover.tsp.core.{Time => CoreTime}

object RowOps:

  implicit class RowOps(private val row: Row) extends AnyVal:

    def getFieldOrThrow(i: Int): AnyRef =
      if row.length > i then row(i)
      else throw new RuntimeException(s"Cannot extract $i from row ${row.mkString}")

    def mkString(sep: String): String = "Row(" + row.mkString(sep) + ")"

    def mkString: String = mkString(", ")

  case class RowTsTimeExtractor(timeIndex: Int, tsMultiplier: Double, fieldId: String) extends TimeExtractor[Row]:

    def apply(r: Row) =
      val millis = r(timeIndex) match
        case d: java.lang.Double => (d * tsMultiplier).toLong
        case f: java.lang.Float  => (f * tsMultiplier).toLong
        case n: java.lang.Number => (n.doubleValue() * tsMultiplier).toLong
        case null                => 0L // TODO: Where can nulls come from?
        case x => sys.error(s"Cannot parse time `$x` from field $fieldId, should be number of millis since 1.1.1970")
      CoreTime(toMillis = millis)

  case class RowIsoTimeExtractor(timeIndex: Int, fieldId: String) extends TimeExtractor[Row]:

    def apply(r: Row) =
      val isoTime = r(timeIndex).toString
      if isoTime == null || isoTime == "" then
        sys.error(s"Cannot parse time `$isoTime` from field $fieldId, should be in ISO 8601 format")
      CoreTime(toMillis = Instant.parse(isoTime).toEpochMilli)

  case class RowSymbolExtractor(fieldIdxMap: Map[String, Int]) extends Extractor[Row, String, Any]:
    def apply[T](r: Row, s: String)(implicit d: Decoder[Any, T]): T = d(r(fieldIdxMap(s)))

  case class RowIdxExtractor() extends Extractor[Row, Int, Any]:
    def apply[T](r: Row, i: Int)(implicit d: Decoder[Any, T]): T = d(r.getFieldOrThrow(i))
