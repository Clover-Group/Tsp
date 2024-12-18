package ru.itclover.tsp.core

import ru.itclover.tsp.core.Time.{MaxWindow, MinWindow}

object Intervals:

  /** Interval abstraction for time measurment in accumulators and some other patterns */
  sealed trait Interval[T]:
    def contains(item: T): Boolean = getRelativePosition(item) == Inside

    def isInfinite: Boolean

    def getRelativePosition(item: T): IntervalPosition

  /** ADT for checking position of item relative to interval */
  sealed trait IntervalPosition extends Product with Serializable

  case object LessThanBegin extends IntervalPosition
  case object GreaterThanEnd extends IntervalPosition
  case object Inside extends IntervalPosition

  /** Inclusive-exclusive interval of time */
  case class TimeInterval(min: Long, max: Long) extends Interval[Long]:

    assert(
      min >= 0 && max >= 0 && max >= min,
      s"Incorrect Timer configuration (min: $min, max: $max)"
    )

    override def contains(w: Long): Boolean = w >= min && w <= max

    override def isInfinite: Boolean = max == MaxWindow.toMillis

    override def getRelativePosition(item: Long): IntervalPosition =
      if item < min then LessThanBegin
      else if item >= max then GreaterThanEnd
      else Inside

    def midpoint: Long = (min + max) / 2

  object TimeInterval:

    // Here, default arguments are really useful, but still TODO: Investigate
    @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
    def apply(min: Window = MinWindow, max: Window = MaxWindow): TimeInterval = TimeInterval(min.toMillis, max.toMillis)

    val MaxInterval = TimeInterval(MaxWindow, MaxWindow)

  /** Simple inclusive-exclusive numeric interval */
  case class NumericInterval[T](start: T, end: Option[T])(implicit numeric: Numeric[T]) extends Interval[T]:

    override def contains(item: T): Boolean = numeric.gteq(item, start) && (end match {
      case Some(e) => numeric.lteq(item, e)
      case None    => true
    })

    override def isInfinite: Boolean = end.isEmpty

    override def getRelativePosition(item: T): IntervalPosition = (start, end) match
      case (s, _) if numeric.lt(item, s)         => LessThanBegin
      case (_, Some(e)) if numeric.gteq(item, e) => GreaterThanEnd
      case _                                     => Inside

  object NumericInterval:
    def more[T: Numeric](start: T) = NumericInterval(start, None)
    def less[T: Numeric](end: T) = NumericInterval(implicitly[Numeric[T]].zero, Some(end))
