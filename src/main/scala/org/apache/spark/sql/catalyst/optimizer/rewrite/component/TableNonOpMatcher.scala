package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher, RewriteContext}

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class TableNonOpMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  override def compare: CompensationExpressions = CompensationExpressions(true, Seq())
}
