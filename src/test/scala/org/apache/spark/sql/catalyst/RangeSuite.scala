package org.apache.spark.sql.catalyst

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.RangeFilter
import org.apache.spark.sql.types.LongType
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class RangeSuite extends FunSuite
  with BeforeAndAfterAll {
  test("range compare") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)),
      LessThan(ar, Literal(20, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(10, LongType))
    assert(rangeCondition.upperBound.get == Literal(20, LongType))
  }

  test("range choose the bigger in lowerBound") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)),
      GreaterThan(ar, Literal(20, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(20, LongType))
    assert(rangeCondition.upperBound == None)
  }

  test("range single") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(10, LongType))
    assert(rangeCondition.upperBound == None)
  }

  test("range choose the bigger in lowerBound with include") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)),
      GreaterThanOrEqual(ar, Literal(20, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(20, LongType))
    assert(rangeCondition.includeLowerBound == true)
    assert(rangeCondition.upperBound == None)
  }

  test("range choose the bigger in lowerBound with include and same value") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)),
      GreaterThanOrEqual(ar, Literal(10, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(10, LongType))
    assert(rangeCondition.includeLowerBound == false)
    assert(rangeCondition.upperBound == None)
  }
  test("range compare with includeUpperBound and not includeLowerBound") {
    val ar = AttributeReference("a", LongType)()
    val items = Seq(GreaterThan(ar, Literal(10, LongType)),
      LessThanOrEqual(ar, Literal(20, LongType)), LessThanOrEqual(ar, Literal(11, LongType)))
    val rangeCondition = RangeFilter.combineAndMergeRangeCondition(items.filter(RangeFilter.rangeCon).map(RangeFilter.convertRangeCon)).head
    assert(rangeCondition.lowerBound.get == Literal(10, LongType))
    assert(rangeCondition.upperBound.get == Literal(11, LongType))
    assert(rangeCondition.includeLowerBound == false)
    assert(rangeCondition.includeUpperBound == true)
  }
}
