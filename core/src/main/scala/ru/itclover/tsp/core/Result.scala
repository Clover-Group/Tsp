package ru.itclover.tsp.core

sealed trait Result[+A] extends Serializable {

  private def get: A = this match {
    case Fail    => throw new RuntimeException("Illegal get on Fail")
    case Wait    => throw new RuntimeException("Illegal get on Wait")
    case Succ(t) => t
  }

  def isFail: Boolean = this match {
    case Fail    => true
    case Wait    => false
    case Succ(_) => false
  }

  def isSuccess: Boolean = this match {
    case Fail    => false
    case Wait    => false
    case Succ(_) => true
  }

  def isWait: Boolean = this match {
    case Fail    => false
    case Wait    => true
    case Succ(_) => false
  }

  @inline final def map[B](f: A => B): Result[B] = this match {
    case Succ(t) => Succ(f(t))
    case Fail    => Fail
    case Wait    => Wait
  }

  @inline final def fold[B](ifEmpty: => B)(f: A => B): B =
    if (!isSuccess) ifEmpty else f(this.get)

  @inline final def flatMap[B](f: A => Result[B]): Result[B] = this match {
    case Succ(t) => f(t)
    case Fail    => Fail
    case Wait    => Wait
  }

  @inline final def getOrElse[B >: A](default: => B): B =
    if (!isSuccess) default else this.get

  @inline final def foreach(f: A => Unit): Unit = if (isSuccess) f(this.get)
}

object Result {

  implicit class OptionToResult[T](private val opt: Option[T]) extends AnyVal {

    def toResult: Result[T] = opt match {
      case None    => Fail
      case Some(t) => Succ(t)
    }

  }

  def fail[T]: Result[T] = Fail
  def wait[T]: Result[T] = Wait
  def succ[T](t: T): Result[T] = Succ(t)
  val succUnit: Result[Unit] = Succ(())
}

case class Succ[T](t: T) extends Result[T]

object Fail extends Result[Nothing]

object Wait extends Result[Nothing]
