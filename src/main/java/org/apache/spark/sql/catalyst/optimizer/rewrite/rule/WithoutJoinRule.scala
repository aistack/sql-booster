package org.apache.spark.sql.catalyst.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite.{GroupByRewrite, PredicateRewrite, ProjectRewrite, TableOrViewRewrite}
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.{GroupByMatcher, PredicateMatcher, ProjectMatcher, TableNonOpMatcher}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project}

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class WithoutJoinRule extends WithoutJoinGroupRule {

  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    val targetViewPlanOption = fetchView(plan)
    if (!targetViewPlanOption.isDefined) return plan
    val targetViewPlan = targetViewPlanOption.get

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
        queryProjectList = projectList
      case Aggregate(groupingExpressions, aggregateExpressions, _) =>
        viewGroupingExpressions = groupingExpressions
        viewAggregateExpressions = aggregateExpressions
    }

    /**
      * Three match/rewrite steps:
      *   1. Predicate
      *   2. GroupBy
      *   3. Project
      *   4. Table(View)
      */
    val pipeline = ArrayBuffer[PipelineItemExecutor]()


    val predicateMatcher = new PredicateMatcher(targetViewPlan, viewProjectList, queryConjunctivePredicates, viewConjunctivePredicates)
    val predicateRewrite = new PredicateRewrite(targetViewPlan)

    pipeline += PipelineItemExecutor(predicateMatcher, predicateRewrite)

    val groupByMatcher = new GroupByMatcher(targetViewPlan, viewAggregateExpressions, queryGroupingExpressions, viewGroupingExpressions)
    val groupByRewrite = new GroupByRewrite(targetViewPlan)


    val projectMatcher = new ProjectMatcher(targetViewPlan, queryProjectList, viewProjectList)
    val projectRewrite = new ProjectRewrite(targetViewPlan)

    pipeline += PipelineItemExecutor(projectMatcher, projectRewrite)


    val tableMatcher = new TableNonOpMatcher()
    val tableRewrite = new TableOrViewRewrite(targetViewPlan)

    pipeline += PipelineItemExecutor(tableMatcher, tableRewrite)

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
