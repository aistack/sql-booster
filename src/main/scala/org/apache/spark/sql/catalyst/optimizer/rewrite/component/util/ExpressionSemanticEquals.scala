package org.apache.spark.sql.catalyst.optimizer.rewrite.component.util

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
object ExpressionSemanticEquals extends RewriteHelper {
  def process(query: Seq[Expression], view: Seq[Expression]) = {
    val (viewLeft, queryLeft, common) = extractTheSameExpressions(view, query)
    ExpressionIntersectResp(queryLeft, viewLeft, common)
  }
}

case class ExpressionIntersectResp(
                                    queryLeft: Seq[Expression],
                                    viewLeft: Seq[Expression],
                                    common: Seq[Expression]
                                  )
