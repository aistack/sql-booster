package org.apache.spark.sql.catalyst.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite.{PredicateReplaceRewrite, ProjectNonOpRewrite, TableOrViewRewrite}
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.{PredicateMatcher, ProjectMatcher, TableNonOpMatcher}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import tech.mlsql.sqlbooster.meta.ViewCatalyst

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
object WithoutJoinGroupRule {
  def apply: WithoutJoinGroupRule = new WithoutJoinGroupRule()
}

class WithoutJoinGroupRule extends RewriteMatchRule {
  override def fetchView(plan: LogicalPlan): Option[LogicalPlan] = {
    val tables = extractTablesFromPlan(plan)
    if (tables.size == 0) return None
    val table = tables.head
    val viewPlan = ViewCatalyst.meta.getCandinateViewsByTable(table) match {
      case Some(viewNames) =>
        val targetViewNameOption = viewNames.filter { viewName =>
          ViewCatalyst.meta.getViewLogicalPlan(viewName) match {
            case Some(viewLogicalPlan) =>
              extractTablesFromPlan(viewLogicalPlan).toSet == Set(table)
            case None => false
          }
        }.headOption

        targetViewNameOption match {
          case Some(targetViewName) =>
            Option(ViewCatalyst.meta.getViewLogicalPlan(targetViewName).get)
          case None => None
        }

      case None => None
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
    if (!targetViewPlanOption.isDefined) return plan
    val targetViewPlan = targetViewPlanOption.get

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

    normalizePlan(targetViewPlan) match {
      case Project(projectList, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewProjectList = projectList
      case Project(projectList, _) =>
        queryProjectList = projectList
    }

    /**
      * Three match/rewrite steps:
      *   1. Project
      *   2. Predicate
      *   3. Table(View)
      */
    val pipeline = ArrayBuffer[PipelineItemExecutor]()

    val projectMatcher = new ProjectMatcher(queryProjectList, viewProjectList)
    val projectRewrite = new ProjectNonOpRewrite()

    pipeline += PipelineItemExecutor(projectMatcher, projectRewrite)


    val predicateMatcher = new PredicateMatcher(queryConjunctivePredicates, viewConjunctivePredicates)
    val predicateRewrite = new PredicateReplaceRewrite()

    pipeline += PipelineItemExecutor(predicateMatcher, predicateRewrite)


    val tableMatcher = new TableNonOpMatcher()
    val tableRewrite = new TableOrViewRewrite(targetViewPlan)

    pipeline += PipelineItemExecutor(tableMatcher, tableRewrite)

    /**
      * When we are rewriting plan, any step fails, we should return the original plan.
      * So we should check the mark in RewritedLogicalPlan is final success or fail.
      */
    LogicalPlanRewritePipeline(pipeline).rewrite(plan) match {
      case RewritedLogicalPlan(_, true) => plan
      case RewritedLogicalPlan(inner, false) => inner

    }

  }


}

