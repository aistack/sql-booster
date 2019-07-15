package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class PredicateReplaceRewrite extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    val newPlan = plan transformDown {
      case a@Filter(condition, child) =>
        RewritedLeafLogicalPlan(Filter(mergeConjunctiveExpressions(_compensationExpressions.compensation), child))
    }
    _back(newPlan)

  }
}
