package ru.itclover.tsp.v2

import cats.Order
import ru.itclover.tsp.v2.Pattern.{Idx, _}
import scala.collection.{mutable => m}
import scala.language.higherKinds

/**
  * Main trait for all patterns, basically just a function from state and events chunk to new a state.
  * @tparam Event underlying data
  * @tparam T Type of the results in the S
  * @tparam S Holds State for the next step AND results (wrong named `queue`)
  * @tparam F Container for state (some simple monad mostly)
  * @tparam Cont Container for yet another chunk of Events
  */
trait Pattern[Event, T, S <: PState[T, S], F[_], Cont[_]] extends ((S, Cont[Event]) => F[S]) with Serializable {
  def initialState(): S
}

trait IdxValue[+T] {
  def index: Idx
  def value: Result[T]
  def start: Idx
  def end: Idx
}

object IdxValue {

  def apply[T](index: Idx, value: Result[T]): IdxValue[T] = new IdxValueSimple[T](index, value)
  def unapply[T](arg: IdxValue[T]): Option[(Idx, Result[T])] = Some(arg.index -> arg.value)

  case class IdxValueSimple[T](index: Idx, value: Result[T]) extends IdxValue[T] {
    override def start: Idx = index
    override def end: Idx = index
  }

  case class IdxValueSegment[T](index: Idx, start: Idx, end: Idx, value: Result[T]) extends IdxValue[T]
}

object Pattern {

  type Idx = Long

  type QI[T] = m.Queue[IdxValue[T]]



  trait IdxExtractor[Event] extends Serializable with Order[Idx] {
    def apply(e: Event): Idx
  }

  // .. TODO Try to fixate TsIdxExtractor for all the patterns somehow
  class TsIdxExtractor[Event](eventToTs: Event => Long) extends IdxExtractor[Event] {
    var counter = 0

    override def apply(e: Event): Idx = {
      counter += 1
      eventToTs(e) * 100000 + counter // .. incr
    }

    override def compare(x: Idx, y: Idx) = idxToTs(x) compare idxToTs(y)


    def idxToTs(idx: Idx): Long = idx / 100000
  }

  object IdxExtractor {
    implicit class GetIdx[T](val event: T) extends AnyVal {
      def index(implicit te: IdxExtractor[T]): Idx = te.apply(event)
    }

    def of[E](f: E => Idx): IdxExtractor[E] = new IdxExtractor[E] {
      override def apply(e: E): Idx = f(e)

      override def compare(x: Idx, y: Idx) = x compare y
    }
  }
}
