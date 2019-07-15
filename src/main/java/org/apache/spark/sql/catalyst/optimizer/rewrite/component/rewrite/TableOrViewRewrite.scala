package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class TableOrViewRewrite(targetViewPlan: LogicalPlan) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    println(plan)
    val newPlan = plan transformDown {
      case SubqueryAlias(_, _) =>
        RewritedLeafLogicalPlan(targetViewPlan)
      case HiveTableRelation(_, _, _) =>
        RewritedLeafLogicalPlan(targetViewPlan)
      case LogicalRelation(_, output, catalogTable, _) =>
        RewritedLeafLogicalPlan(targetViewPlan)
    }

    _back(newPlan)

  }
}
