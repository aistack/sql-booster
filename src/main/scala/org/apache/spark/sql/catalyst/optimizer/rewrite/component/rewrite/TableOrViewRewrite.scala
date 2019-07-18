package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class TableOrViewRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    val finalTable = rewriteContext.viewLogicalPlan.get().tableLogicalPlan match {
      case Project(_, child) => child
      case _ => rewriteContext.viewLogicalPlan.get().tableLogicalPlan
    }
    val newPlan = plan transformDown {
      case SubqueryAlias(_, _) =>
        RewritedLeafLogicalPlan(finalTable)
      case HiveTableRelation(_, _, _) =>
        RewritedLeafLogicalPlan(finalTable)
      case LogicalRelation(_, output, catalogTable, _) =>
        RewritedLeafLogicalPlan(finalTable)
    }

    _back(newPlan)

  }
}
