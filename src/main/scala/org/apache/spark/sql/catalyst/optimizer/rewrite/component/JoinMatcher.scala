package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, SubqueryAlias}

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
class JoinMatcher(rewriteContext: RewriteContext
                 ) extends ExpressionMatcher {
  override def compare: CompensationExpressions = {


    val viewJoin = rewriteContext.processedComponent.get().viewJoins.head
    val queryJoin = rewriteContext.processedComponent.get().queryJoins.head
    // since the prediate condition will be pushed down into Join filter,
    // but we have compare them in Predicate Matcher/Rewrite step, so when compare Join,
    // we should clean the filter from Join
    if (!sameJoinPlan(cleanJoinFilter(viewJoin), cleanJoinFilter(queryJoin))) return RewriteFail.JOIN_UNMATCH(this)
    CompensationExpressions(true, Seq())
  }

  def cleanJoinFilter(join: Join) = {
    val newPlan = join transformUp {
      case a@Filter(_, child) =>
        child
      case SubqueryAlias(_, a@SubqueryAlias(_, _)) =>
        a
      case a@Join(_, _, _, condition) =>
        if (condition.isDefined) {
          val newConditions = condition.get transformUp {
            case a@AttributeReference(name, dataType, nullable, metadata) =>
              AttributeReference(name, dataType, nullable, metadata)(a.exprId, Seq())
          }
          a.copy(condition = Option(newConditions))

        } else a

    }
    newPlan
  }

}
