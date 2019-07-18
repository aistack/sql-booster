package org.apache.spark.sql.catalyst.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.PreOptimizeRewrite
import org.apache.spark.sql.catalyst.optimizer.rewrite.component._
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.mlsql.sqlbooster.meta.ViewCatalyst

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */

object SPGJRule {
  def apply: SPGJRule = new SPGJRule()
}

class SPGJRule extends RewriteMatchRule {

  /**
    *
    * @param plan
    * @return
    */
  override def fetchView(plan: LogicalPlan, rewriteContext: RewriteContext): Seq[ViewLogicalPlan] = {
    require(plan.resolved, "LogicalPlan must be resolved.")

    if (!isJoinExists(plan)) return Seq()

    // get all tables in join and the first table
    val tables = extractTablesFromPlan(plan)
    if (tables.size == 0) return Seq()

    var mainTableLogicalPlan: LogicalPlan = null

    plan transformUp {
      case a@Join(_, _, _, _) =>
        a.left transformUp {
          case a@SubqueryAlias(_, child@LogicalRelation(_, _, _, _)) =>
            mainTableLogicalPlan = a
            a
          case a@SubqueryAlias(_, child@LogicalRDD(_, _, _, _, _)) =>
            mainTableLogicalPlan = a
            a
        }
        a
    }

    val mainTable = extractTablesFromPlan(mainTableLogicalPlan).head

    val viewPlan = ViewCatalyst.meta.getCandidateViewsByTable(mainTable) match {
      case Some(viewNames) =>
        viewNames.filter { viewName =>
          ViewCatalyst.meta.getViewCreateLogicalPlan(viewName) match {
            case Some(viewLogicalPlan) =>
              extractTablesFromPlan(viewLogicalPlan).toSet == tables.toSet
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

  override def rewrite(_plan: LogicalPlan, rewriteContext: RewriteContext): LogicalPlan = {
    val plan = PreOptimizeRewrite.execute(_plan)
    var targetViewPlanOption = fetchView(plan, rewriteContext)
    if (targetViewPlanOption.isEmpty) return plan

    targetViewPlanOption = targetViewPlanOption.map(f =>
      f.copy(viewCreateLogicalPlan = PreOptimizeRewrite.execute(f.viewCreateLogicalPlan)))

    var shouldBreak = false
    var finalPlan = RewritedLogicalPlan(plan, true)

    targetViewPlanOption.foreach { targetViewPlan =>
      if (!shouldBreak) {
        rewriteContext.viewLogicalPlan.set(targetViewPlan)
        val res = _rewrite(plan, rewriteContext)
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

  def _rewrite(plan: LogicalPlan, rewriteContext: RewriteContext): LogicalPlan = {

    generateRewriteContext(plan, rewriteContext)

    val pipeline = buildPipeline(rewriteContext: RewriteContext, Seq(
      new PredicateMatcher(rewriteContext),
      new SPGJPredicateRewrite(rewriteContext),
      new GroupByMatcher(rewriteContext),
      new GroupByRewrite(rewriteContext),
      new AggMatcher(rewriteContext),
      new AggRewrite(rewriteContext),
      new JoinMatcher(rewriteContext),
      new JoinRewrite(rewriteContext),
      new ProjectMatcher(rewriteContext),
      new ProjectRewrite(rewriteContext)

    ))

    /**
      * When we are rewriting plan, any step fails, we should return the original plan.
      * So we should check the mark in RewritedLogicalPlan is final success or fail.
      */
    LogicalPlanRewritePipeline(pipeline).rewrite(plan)
  }

  
}


