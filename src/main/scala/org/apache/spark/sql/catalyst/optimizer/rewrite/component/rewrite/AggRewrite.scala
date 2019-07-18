package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class AggRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    val projectOrAggList = rewriteContext.viewLogicalPlan.get().tableLogicalPlan.output

    val newExpressions = _compensationExpressions.compensation.map { expr =>
      expr transformDown {
        case a@AttributeReference(name, dt, _, _) =>
          val newAr = extractAttributeReferenceFromFirstLevel(projectOrAggList).filter(f => attributeReferenceEqual(a, f)).head
          rewriteContext.replacedARMapping += (a.withQualifier(Seq()) -> newAr)
          newAr
      }
    }.map(_.asInstanceOf[NamedExpression])
    val newPlan = plan transformDown {
      case Aggregate(groupingExpressions, _, child) =>
        RewritedLeafLogicalPlan(Aggregate(groupingExpressions, newExpressions, child))
    }
    _back(newPlan)
  }
}
