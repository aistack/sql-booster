package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan, ViewLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class PredicateRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    val projectOrAggList = rewriteContext.viewLogicalPlan.tableLogicalPlan.output

    val newExpressions = _compensationExpressions.compensation.map { expr =>
      expr transformDown {
        case a@AttributeReference(name, dt, _, _) =>
          extractAttributeReferenceFromFirstLevel(projectOrAggList).filter(f => attributeReferenceEqual(a, f)).head
      }
    }


    val newPlan = plan transformDown {
      case a@Filter(condition, child) =>
        if (newExpressions.isEmpty) {
          RewritedLeafLogicalPlan(child)
        } else {
          RewritedLeafLogicalPlan(Filter(mergeConjunctiveExpressions(newExpressions), child))
        }

    }
    _back(newPlan)
  }
}
