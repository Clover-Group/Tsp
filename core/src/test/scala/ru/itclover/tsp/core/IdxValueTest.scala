package ru.itclover.tsp.core

import cats.instances.long._
import org.scalatest.{FlatSpec, Matchers}

class IdxValueTest extends FlatSpec with Matchers {

  it should "check intersections of IdxValues" in {
    val iv1 = IdxValue(0, 10, Result.fail)
    val iv2 = IdxValue(5, 15, Result.fail)
    val iv3 = IdxValue(10, 20, Result.fail)
    val iv4 = IdxValue(15, 25, Result.fail)

    iv1.intersects(iv1) shouldBe true
    iv1.intersects(iv2) shouldBe true
    iv1.intersects(iv3) shouldBe true
    iv1.intersects(iv4) shouldBe false

    iv2.intersects(iv1) shouldBe true
    iv3.intersects(iv1) shouldBe true
    iv4.intersects(iv1) shouldBe false

  }
}