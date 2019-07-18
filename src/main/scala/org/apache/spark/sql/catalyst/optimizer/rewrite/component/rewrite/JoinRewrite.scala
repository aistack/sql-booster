package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
class JoinRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    plan transformUp {
      case Join(_, _, _, _) => rewriteContext.viewLogicalPlan.tableLogicalPlan match {
        case Project(_, child) => child
        case _ => rewriteContext.viewLogicalPlan.tableLogicalPlan
      }
    }
  }
}
