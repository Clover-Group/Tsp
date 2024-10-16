package ru.itclover.tsp.core

import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.QI

import scala.annotation.tailrec

/*
Joins together sequential outputs of the inner pattern with the same value. It reduces amount of produced results.
 */
case class SegmentizerPattern[Event, T, InnerState](inner: Pattern[Event, InnerState, T])
    extends Pattern[Event, SegmentizerPState[InnerState, T], T] {
//todo tests

  @tailrec
  private def inner(q: QI[T], last: IdxValue[T], resultQ: QI[T]): (QI[T], Boolean) = {
    q.dequeueOption() match {
      case None => (resultQ.enqueue(last), last.value.isWait)
      case Some((head, tail)) => {
        // Any value should be united with the previous Wait value
        if (head.value.equals(last.value) || last.value.isWait) {
          inner(tail, head.copy(start = last.start), resultQ)
        } else {
          inner(tail, head, resultQ.enqueue(last))
        }
      }
    }
  }

  override def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: SegmentizerPState[InnerState, T],
    queue: PQueue[T],
    event: Cont[Event]
  ): F[(SegmentizerPState[InnerState, T], PQueue[T])] =
    inner(oldState.innerState, oldState.innerQueue, event).map {
      case (innerResult, innerQueue) => {
        innerQueue.dequeueOption() match {
          case None => oldState.copy(innerState = innerResult) -> queue
          case Some((head, tail)) =>
            val (newQueue, lastWait) = inner(tail, head, queue) // do not inline!
            SegmentizerPState(innerResult, PQueue.empty[T], lastWait) -> newQueue
        }
      }
    }

  override def initialState(): SegmentizerPState[InnerState, T] =
    SegmentizerPState(inner.initialState(), PQueue.empty, false)

}

case class SegmentizerPState[InnerState, T](innerState: InnerState, innerQueue: QI[T], lastWait: Boolean)
