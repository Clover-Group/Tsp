package ru.itclover.tsp.streaming

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ru.itclover.tsp.streaming.io._
import ru.itclover.tsp.streaming.mappers.PatternsToRowMapper
import ru.itclover.tsp.StreamSource.Row

// Pattern converting from value returns Any by design
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class PatternsToRowMapperTest extends AnyFlatSpec with Matchers:

  val eventSchema: NewRowSchema = NewRowSchema(
    Map(
      "int32Value" -> IntESValue("int32", 42)
    )
  )

  val mapper: PatternsToRowMapper[Row, String] = PatternsToRowMapper(eventSchema)

  "PatternsToRowMapper" should "convert from integer" in:
    mapper.convertFromInt(0, "boolean") shouldBe false
    mapper.convertFromInt(1, "boolean") shouldBe true
    mapper.convertFromInt(101, "int8") shouldBe 101.toByte
    mapper.convertFromInt(102, "int16") shouldBe 102.toShort
    mapper.convertFromInt(103, "int32") shouldBe 103.toInt
    mapper.convertFromInt(104, "int64") shouldBe 104.toLong
    mapper.convertFromInt(105, "float32") shouldBe 105.0f
    mapper.convertFromInt(106, "float64") shouldBe 106.0
    mapper.convertFromInt(107, "string") shouldBe "107"
    mapper.convertFromInt(108, "object") shouldBe 108

  "PatternsToRowMapper" should "convert from string" in:
    mapper.convertFromString("0", "boolean") shouldBe false
    mapper.convertFromString("1", "boolean") shouldBe true
    mapper.convertFromString("false", "boolean") shouldBe false
    mapper.convertFromString("true", "boolean") shouldBe true
    mapper.convertFromString("off", "boolean") shouldBe false
    mapper.convertFromString("on", "boolean") shouldBe true
    mapper.convertFromString("no", "boolean") shouldBe false
    mapper.convertFromString("yes", "boolean") shouldBe true

    mapper.convertFromString("101", "int8") shouldBe 101.toByte
    mapper.convertFromString("102", "int16") shouldBe 102.toShort
    mapper.convertFromString("103", "int32") shouldBe 103.toInt
    mapper.convertFromString("104", "int64") shouldBe 104.toLong
    mapper.convertFromString("105", "float32") shouldBe 105.0f
    mapper.convertFromString("106", "float64") shouldBe 106.0
    mapper.convertFromString("107", "string") shouldBe "107"
    mapper.convertFromString("108", "object") shouldBe "108"

  "PatternsToRowMapper" should "convert from float" in:
    mapper.convertFromFloat(0.0, "boolean") shouldBe false
    mapper.convertFromFloat(1.0, "boolean") shouldBe true
    mapper.convertFromFloat(101.0, "int8") shouldBe 101.toByte
    mapper.convertFromFloat(102.0, "int16") shouldBe 102.toShort
    mapper.convertFromFloat(103.0, "int32") shouldBe 103.toInt
    mapper.convertFromFloat(104.0, "int64") shouldBe 104.toLong
    mapper.convertFromFloat(105.0, "float32") shouldBe 105.0f
    mapper.convertFromFloat(106.0, "float64") shouldBe 106.0
    mapper.convertFromFloat(107.0, "string") shouldBe "107.0"
    mapper.convertFromFloat(108.0, "object") shouldBe 108.0
