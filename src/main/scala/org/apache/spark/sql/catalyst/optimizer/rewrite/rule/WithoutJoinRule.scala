package org.apache.spark.sql.catalyst.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite.{AggRewrite, GroupByRewrite, PredicateRewrite, TableOrViewRewrite}
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.{AggMatcher, GroupByMatcher, PredicateMatcher, TableNonOpMatcher}
import org.apache.spark.sql.catalyst.plans.logical._
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class WithoutJoinRule extends RewriteMatchRule {


  override def fetchView(plan: LogicalPlan): Seq[ViewLogicalPlan] = {
    if (isJoinExists(plan)) return Seq()

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

    var queryGroupingExpressions: Seq[Expression] = Seq()
    var viewGroupingExpressions: Seq[Expression] = Seq()

    var queryAggregateExpressions: Seq[Expression] = Seq()
    var viewAggregateExpressions: Seq[Expression] = Seq()

    // check projectList and where condition
    normalizePlan(plan) match {
      case Project(projectList, Filter(condition, _)) =>
        queryConjunctivePredicates = splitConjunctivePredicates(condition)
        queryProjectList = projectList
      case Project(projectList, _) =>
        queryProjectList = projectList
      case Aggregate(groupingExpressions, aggregateExpressions, _) =>
        queryGroupingExpressions = groupingExpressions
        queryAggregateExpressions = aggregateExpressions
    }

    normalizePlan(targetViewPlan.viewCreateLogicalPlan) match {
      case Project(projectList, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewProjectList = projectList
      case Project(projectList, _) =>
        viewProjectList = projectList
      case Aggregate(groupingExpressions, aggregateExpressions, _) =>
        viewGroupingExpressions = groupingExpressions
        viewAggregateExpressions = aggregateExpressions
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
      *   2. GroupBy
      *   3. Project
      *   4. Table(View)
      */
    val pipeline = buildPipeline(rewriteContext: RewriteContext, Seq(
      new PredicateMatcher(rewriteContext),
      new PredicateRewrite(rewriteContext),
      new GroupByMatcher(rewriteContext),
      new GroupByRewrite(rewriteContext),
      new AggMatcher(rewriteContext),
      new AggRewrite(rewriteContext),
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

object WithoutJoinRule {
  def apply: WithoutJoinRule = new WithoutJoinRule()
}
