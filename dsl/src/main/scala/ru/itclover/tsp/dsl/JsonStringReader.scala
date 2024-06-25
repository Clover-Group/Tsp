package ru.itclover.tsp.dsl

import spray.json.*

object JsonStringReader extends JsonReader[String] {

  override def read(json: JsValue): String = json match
    case JsArray(elements) => s"[${elements.map(read).mkString(",")}]"
    case JsNull            => ""
    case JsNumber(value)   => value.toString
    case JsObject(fields)  => s"{${fields.map { case (k, v) => s""" "k": ${read(v)} """ }.mkString(",")}}"
    case JsString(value)   => value
    case JsFalse           => "false"
    case JsTrue            => "true"

}
