package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class GroupByRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    val projectOrAggList = rewriteContext.viewLogicalPlan.get().tableLogicalPlan.output

    val newExpressions = _compensationExpressions.compensation.map { expr =>
      expr transformDown {
        case a@AttributeReference(name, dt, _, _) =>
          extractAttributeReferenceFromFirstLevel(projectOrAggList).filter(f => attributeReferenceEqual(a, f)).head
      }
    }


    val newPlan = plan transformDown {
      case Aggregate(_, aggregateExpressions, child) =>
        RewritedLeafLogicalPlan(Aggregate(newExpressions, aggregateExpressions, child))
    }
    _back(newPlan)
  }
}