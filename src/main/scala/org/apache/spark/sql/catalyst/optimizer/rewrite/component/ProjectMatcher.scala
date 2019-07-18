package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.optimizer.rewrite.component.util.{ExpressionIntersectResp, ExpressionSemanticEquals}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule._

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class ProjectMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  /**
    *
    * @param query the project expression list in query
    * @param view  the project expression list in view
    * @return
    *
    * We should make sure all query project list isSubSet of view project list.
    *
    *
    */
  override def compare: CompensationExpressions = {

    val query = rewriteContext.processedComponent.get().queryProjectList
    val view = rewriteContext.processedComponent.get().viewProjectList
    val ExpressionIntersectResp(queryLeft, viewLeft, _) = ExpressionSemanticEquals.process(query, view)
    // for now, we must make sure the queryLeft's columns(not alias) all in viewLeft.columns(not alias)
    val queryColumns = queryLeft.flatMap(extractAttributeReference)
    val viewColumns = viewLeft.flatMap(extractAttributeReference)

    val ExpressionIntersectResp(queryColumnsLeft, viewColumnsLeft, _) = ExpressionSemanticEquals.process(queryColumns, viewColumns)
    if (queryColumnsLeft.size > 0) return RewriteFail.PROJECT_UNMATCH(this)
    CompensationExpressions(true, Seq())
  }


}
