package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
class SPGJPredicateRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    val projectOrAggList = rewriteContext.viewLogicalPlan.get().tableLogicalPlan.output

    val newExpressions = _compensationExpressions.compensation.map { expr =>
      expr transformDown {
        case a@AttributeReference(name, dt, _, _) =>
          extractAttributeReferenceFromFirstLevel(projectOrAggList).filter(f => attributeReferenceEqual(a, f)).head
      }
    }

    //clean filter and then add new filter before Join
    var newPlan = plan transformDown {
      case a@Filter(condition, child) =>
        child
    }

    var lastJoin: Join = null
    newPlan = plan transformUp {
      case a@Join(_, _, _, _) =>
        lastJoin = a
        a
    }

    newPlan = plan transformDown {
      case a@Join(_, _, _, _) =>
        if (a == lastJoin) {
          RewritedLeafLogicalPlan(Filter(mergeConjunctiveExpressions(newExpressions), a))
        } else a
    }


    _back(newPlan)

  }
}