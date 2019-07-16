package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher, RewriteFail, ViewLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join}

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
class JoinMatcher(viewLogicalPlan: ViewLogicalPlan, viewJoin: Join, queryJoin: Join
                 ) extends ExpressionMatcher {
  override def compare: CompensationExpressions = {
    // since the prediate condition will be pushed down into Join filter,
    // but we have compare them in Predicate Matcher/Rewrite step, so when compare Join,
    // we should clean the filter from Join
    if (!sameJoinPlan(cleanJoinFilter(viewJoin), cleanJoinFilter(queryJoin))) return RewriteFail.JOIN_UNMATCH(this)
    CompensationExpressions(true, Seq())
  }

  def cleanJoinFilter(join: Join) = {
    join transformDown {
      case a@Filter(_, child) =>
        child
    }
  }
}
