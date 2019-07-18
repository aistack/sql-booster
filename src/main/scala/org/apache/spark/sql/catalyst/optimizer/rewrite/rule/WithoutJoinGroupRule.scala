package org.apache.spark.sql.catalyst.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rewrite.component._
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite._
import org.apache.spark.sql.catalyst.plans.logical._
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
object WithoutJoinGroupRule {
  def apply: WithoutJoinGroupRule = new WithoutJoinGroupRule()
}


class WithoutJoinGroupRule extends RewriteMatchRule {
  override def fetchView(plan: LogicalPlan): Seq[ViewLogicalPlan] = {


    if (isAggExistsExists(plan) || isJoinExists(plan)) return Seq()

    val tables = extractTablesFromPlan(plan)
    if (tables.size == 0) return Seq()
    val table = tables.head
    val viewPlan = ViewCatalyst.meta.getCandidateViewsByTable(table) match {
      case Some(viewNames) =>
        viewNames.filter { viewName =>
          ViewCatalyst.meta.getViewCreateLogicalPlan(viewName) match {
            case Some(viewLogicalPlan) =>
              extractTablesFromPlan(viewLogicalPlan).toSet == Set(table)
            case None => false
          }
        }.map { targetViewName =>
          ViewLogicalPlan(
            ViewCatalyst.meta.getViewLogicalPlan(targetViewName).get,
            ViewCatalyst.meta.getViewCreateLogicalPlan(targetViewName).get)
        }.toSeq
      case None => Seq()


    }


    viewPlan
  }


  /**
    * query: select * from a,b where a.name=b.name and a.name2="jack" and b.jack="wow";
    * view: a_view= select * from a,b where a.name=b.name and a.name2="jack" ;
    * target: select * from a_view where  b.jack="wow"
    *
    * step 0: tables equal check
    * step 1: View equivalence classes:
    * query: PE:{a.name,b.name}
    * NPE: {a.name2="jack"} {b.jack="wow"}
    * view: PE: {a.name,b.name},NPE: {a.name2="jack"}
    *
    * step2 QPE < VPE, and QNPE < VNPE. We should normalize the PE make sure a=b equal to b=a, and
    * compare the NPE with Range Check, the others just check exactly
    *
    * step3: output columns check
    *
    * @param plan
    * @return
    */
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    val targetViewPlanOption = fetchView(plan)
    if (targetViewPlanOption.isEmpty) return plan

    var shouldBreak = false
    var finalPlan = RewritedLogicalPlan(plan, true)

    targetViewPlanOption.foreach { targetViewPlan =>
      if (!shouldBreak) {
        val res = _rewrite(plan, targetViewPlan)
        res match {
          case a@RewritedLogicalPlan(_, true) =>
            finalPlan = a
          case a@RewritedLogicalPlan(_, false) =>
            finalPlan = a
            shouldBreak = true
        }
      }
    }
    finalPlan
  }

  def _rewrite(plan: LogicalPlan, targetViewPlan: ViewLogicalPlan): LogicalPlan = {

    var queryConjunctivePredicates: Seq[Expression] = Seq()
    var viewConjunctivePredicates: Seq[Expression] = Seq()

    var queryProjectList: Seq[Expression] = Seq()
    var viewProjectList: Seq[Expression] = Seq()

    // check projectList and where condition
    normalizePlan(plan) match {
      case Project(projectList, Filter(condition, _)) =>
        queryConjunctivePredicates = splitConjunctivePredicates(condition)
        queryProjectList = projectList
      case Project(projectList, _) =>
        queryProjectList = projectList
    }

    normalizePlan(targetViewPlan.viewCreateLogicalPlan) match {
      case Project(projectList, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewProjectList = projectList
      case Project(projectList, _) =>
        viewProjectList = projectList
    }


    val rewriteContext = new RewriteContext(targetViewPlan, ProcessedComponent(
      queryConjunctivePredicates,
      viewConjunctivePredicates,
      queryProjectList,
      viewProjectList,
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq(),
      Seq()
    ))

    /**
      * Three match/rewrite steps:
      *   1. Predicate
      *   2. Project
      *   3. Table(View)
      */
    val pipeline = buildPipeline(rewriteContext: RewriteContext, Seq(
      new PredicateMatcher(rewriteContext),
      new PredicateRewrite(rewriteContext),
      new ProjectMatcher(rewriteContext),
      new ProjectRewrite(rewriteContext),
      new TableNonOpMatcher(rewriteContext),
      new TableOrViewRewrite(targetViewPlan)

    ))

    /**
      * When we are rewriting plan, any step fails, we should return the original plan.
      * So we should check the mark in RewritedLogicalPlan is final success or fail.
      */
    LogicalPlanRewritePipeline(pipeline).rewrite(plan)

  }


}

