package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.rewrite.component.{CompensationExpressions, PredicateMatcher}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.mlsql.sqlbooster.meta.ViewCatalyst

import scala.collection.mutable.ArrayBuffer

/**
  * References:
  * [GL01] Jonathan Goldstein and Per-åke Larson.
  * Optimizing queries using materialized views: A practical, scalable solution. In Proc. ACM SIGMOD Conf., 2001.
  */
object RewriteTableToViews extends Rule[LogicalPlan] with PredicateHelper {
  val batches = ArrayBuffer[WithoutJoinGroupRule](
    WithoutJoinGroupRule.apply
  )

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (isSPJG(plan)) {
      rewrite(plan)
    } else {
      plan.transformDown {
        case a if isSPJG(a) => rewrite(a)
      }
    }

  }

  private def rewrite(plan: LogicalPlan) = {
    // this plan is SPJG, but the first step is check whether we can rewrite it
    var rewritePlan = plan
    batches.foreach { rewriter =>
      rewritePlan = rewriter.rewrite(rewritePlan)
    }
    rewritePlan
  }

  /**
    * check the plan is whether a basic sql pattern
    * only contains select(filter)/agg/project/join/group.
    *
    * @param plan
    * @return
    */
  private def isSPJG(plan: LogicalPlan) = {
    plan match {

      case p@Project(_, Join(_, _, _, _)) => true
      case p@Project(_, Filter(_, Join(_, _, _, _))) => true
      case p@Aggregate(_, _, Filter(_, Join(_, _, _, _))) => true
      case p@Project(_, Filter(_, _)) => true
      case p@Aggregate(_, _, Join(_, _, _, _)) => true
      case p@Filter(_, _) => true
      case _ => false
    }
  }
}

trait RewriteMatchRule extends RewriteHelper {
  def fetchView(plan: LogicalPlan): Option[LogicalPlan]

  def rewrite(plan: LogicalPlan): LogicalPlan


}

object WithoutJoinGroupRule {
  def apply: WithoutJoinGroupRule = new WithoutJoinGroupRule()
}

class WithJoinGroupRule extends RewriteMatchRule {
  override def fetchView(plan: LogicalPlan): Option[LogicalPlan] = None

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
    plan
  }
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
    * query: select * from a where a.name2="jack" and a.jack="wow";
    * view: a_view= select * from a.name2="jack" ;
    * target: select * from a_view where  a.jack="wow"
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

    val whereMatcher = new PredicateMatcher()
    val compensationExpressions = whereMatcher.compare(queryConjunctivePredicates, viewConjunctivePredicates)

    val newplan = compensationExpressions match {
      case CompensationExpressions(true, compensation) =>
        val tempPlan = plan transformDown {
          case SubqueryAlias(_, _) =>
            ViewLogicalPlan(targetViewPlan)
          case HiveTableRelation(_, _, _) =>
            ViewLogicalPlan(targetViewPlan)
          case LogicalRelation(_, output, catalogTable, _) =>
            ViewLogicalPlan(targetViewPlan)
          case a@Filter(condition, child) =>
            Filter(mergeConjunctiveExpressions(compensationExpressions.compensation), child)
        }
        //chagne back
        tempPlan transformDown {
          case ViewLogicalPlan(inner) => inner
        }
      // turn back
      case CompensationExpressions(false, compensation) => plan
    }

    newplan
  }


}

case class ViewLogicalPlan(inner: LogicalPlan) extends LogicalPlan {
  override def output: Seq[Attribute] = inner.output

  override def children: Seq[LogicalPlan] = inner.children
}


