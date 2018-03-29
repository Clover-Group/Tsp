package ru.itclover.streammachine

import com.typesafe.scalalogging.Logger
import ru.itclover.streammachine.core.PhaseResult._
import ru.itclover.streammachine.core.Time.TimeExtractor
import ru.itclover.streammachine.core.{PhaseParser, PhaseResult}

import scala.collection.mutable

case class StateMachineMapper[Event, State, PhaseOut, MapperOut]
  (phaseParser: PhaseParser[Event, State, PhaseOut], mapResults: ResultMapper[Event, PhaseOut, MapperOut])
  extends AbstractStateMachineMapper[Event, State, PhaseOut]
{
  val log: Logger = Logger[StateMachineMapper[Event, State, PhaseOut, MapperOut]]

  private var states: Seq[State] = Vector.empty

  private val collector = mutable.ListBuffer.empty[TerminalResult[MapperOut]]


  def apply(event: Event): this.type = {
    val (results, newStates) = process(event, states)

    mapResults(event, results).foreach(x => collector.append(x))

    states = newStates

    this
  }

  def result: Vector[TerminalResult[MapperOut]] = collector.toVector

  /** @inheritdoc */
  override def doProcessOldState(event: Event) = true // TODO

  override def isEventTerminal(event: Event) = false // TODO
}