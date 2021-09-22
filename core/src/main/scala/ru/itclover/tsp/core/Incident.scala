package ru.itclover.tsp.core

import cats.Semigroup

import java.util.UUID

/**
  * Represents found pattern
  *
  * @param id from input conf
  * @param maxWindowMs maximum time-window (accum, aggregation) inside
  * @param segment bounds of incident
  * @param forwardedFields which fields need to push to sink
  */
case class Incident(
  id: String,
  incidentUUID: UUID,
  patternId: Int,
  maxWindowMs: Long,
  segment: Segment,
  @deprecated("Unused and scheduled to be removed in 0.17", since = "TSP 0.16")
  forwardedFields: Seq[(String, String)],
  patternUnit: Int,
  patternSubunit: Int,
  @deprecated("Unused and scheduled to be removed in 0.17", since = "TSP 0.16")
  patternPayload: Seq[(String, String)]
) extends Product
    with Serializable

object IncidentInstances {

  implicit def semigroup: Semigroup[Incident] = new Semigroup[Incident] {
    override def combine(a: Incident, b: Incident): Incident = {
      val from =
        if (a.segment.from.toMillis > b.segment.from.toMillis) b.segment.from
        else a.segment.from
      val to =
        if (a.segment.to.toMillis > b.segment.to.toMillis) a.segment.to
        else b.segment.to
      Incident(
        b.id,
        b.incidentUUID,
        b.patternId,
        b.maxWindowMs,
        Segment(from, to),
        b.forwardedFields,
        b.patternUnit,
        b.patternSubunit,
        b.patternPayload
      )
    }
  }
}
