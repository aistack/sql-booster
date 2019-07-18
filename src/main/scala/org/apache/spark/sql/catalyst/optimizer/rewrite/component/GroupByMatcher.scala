package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule._

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class GroupByMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  override def compare: CompensationExpressions = {
    /**
      * Query:
      *
      * SELECT deptno
      * FROM emps
      * WHERE deptno > 10
      * GROUP BY deptno
      *
      * View:
      *
      * SELECT empid, deptno
      * FROM emps
      * WHERE deptno > 5
      * GROUP BY empid, deptno
      *
      * Target:
      *
      * SELECT deptno
      * FROM mv
      * WHERE deptno > 10
      * GROUP BY deptno
      *
      * then  query  isSubSet of view . Please take care of the order in group by.
      */
    val query = rewriteContext.processedComponent.get().queryGroupingExpressions
    val view = rewriteContext.processedComponent.get().viewGroupingExpressions
    val viewAggregateExpressions = rewriteContext.processedComponent.get().viewAggregateExpressions

    if (query.size > view.size) return RewriteFail.GROUP_BY_SIZE_UNMATCH(this)
    if (!isSubSetOf(query, view)) return RewriteFail.GROUP_BY_SIZE_UNMATCH(this)

    // again make sure the columns in queryLeft is also in view project/agg

    val viewAttrs = extractAttributeReferenceFromFirstLevel(viewAggregateExpressions)

    val compensationCondAllInViewProjectList = isSubSetOf(query.flatMap(extractAttributeReference), viewAttrs)

    if (!compensationCondAllInViewProjectList) return RewriteFail.GROUP_BY_COLUMNS_NOT_IN_VIEW_PROJECT_OR_AGG(this)

    CompensationExpressions(true, query)

  }

  private def extractTheSameExpressionsOrder(view: Seq[Expression], query: Seq[Expression]) = {
    val viewLeft = ArrayBuffer[Expression](view: _*)
    val queryLeft = ArrayBuffer[Expression](query: _*)
    val common = ArrayBuffer[Expression]()

    (0 until view.size).foreach { index =>
      if (view(index).semanticEquals(query(index))) {
        common += view(index)
        viewLeft -= view(index)
        queryLeft -= query(index)
      }
    }

    (viewLeft, queryLeft, common)
  }
}


