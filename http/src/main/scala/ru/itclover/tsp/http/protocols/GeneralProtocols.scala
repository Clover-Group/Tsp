package ru.itclover.tsp.http.protocols

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import ru.itclover.tsp.core.RawPattern
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat

trait GeneralProtocols extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val rawPatternFmt: RootJsonFormat[RawPattern] = jsonFormat4(RawPattern.apply)
}
