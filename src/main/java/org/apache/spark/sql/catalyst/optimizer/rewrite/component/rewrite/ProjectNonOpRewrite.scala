package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.LogicalPlanRewrite
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class ProjectNonOpRewrite extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    plan
  }
}
