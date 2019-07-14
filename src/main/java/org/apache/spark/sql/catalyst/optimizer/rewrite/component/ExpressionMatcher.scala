package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper

/**
  * 2019-07-13 WilliamZhu(allwefantasy@gmail.com)
  */
trait ExpressionMatcher extends ExpressionMatcherHelper {
  val DEFAULT = CompensationExpressions(false, Seq())

  def compare(query: Seq[Expression], view: Seq[Expression]): CompensationExpressions
}

trait ExpressionMatcherHelper extends RewriteHelper {
  def isSubSetOf(e1: Seq[Expression], e2: Seq[Expression]) = {
    val zipCount = Math.min(e1.size, e2.size)
    (0 until zipCount).map { index => if (e1(index).semanticEquals(e2(index))) 0 else 1 }.sum == 0
  }

  def subset[T](e1: Seq[T], e2: Seq[T]) = {
    assert(e1.size >= e2.size)
    if (e1.size == 0) Seq[Expression]()
    e1.slice(e2.size, e1.size)
  }
}

case class CompensationExpressions(isRewriteSuccess: Boolean, compensation: Seq[Expression])
