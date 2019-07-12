package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, EqualTo, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.sources._
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * References:
  * [GL01] Jonathan Goldstein and Per-åke Larson.
  * Optimizing queries using materialized views: A practical, scalable solution. In Proc. ACM SIGMOD Conf., 2001.
  */
object RewriteTableToView extends Rule[LogicalPlan] with PredicateHelper {

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
    plan
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
      case _ => false
    }
  }
}

trait RewriteMatchRule extends RewriteHelper {
  def fetchView(plan: LogicalPlan): Option[LogicalPlan]

  def rewrite(plan: LogicalPlan): LogicalPlan


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
    val viewPlan = plan match {
      case p@Project(_, Filter(_, table@SubqueryAlias(_, _))) =>
        ViewCatalyst.meta.getCandinateViewsByTable(table.name.unquotedString) match {
          case Some(viewNames) =>
            val targetViewNameOption = viewNames.filter { viewName =>
              ViewCatalyst.meta.getViewLogicalPlan(viewName) match {
                case Some(viewLogicalPlan) =>
                  extractTablesFromPlan(viewLogicalPlan) == Set(table.name.unquotedString)
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
      case _ => None
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
    }

    normalizePlan(targetViewPlan) match {
      case Project(projectList, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewProjectList = projectList
    }

    // val compsentCondition = queryEquals.toSet -- viewEquals.toSet
    plan
  }

  val equalCon = (f: Expression) => {
    f.isInstanceOf[EqualNullSafe] || f.isInstanceOf[EqualTo]
  }
  val rangeCon = (f: Expression) => {
    f.isInstanceOf[GreaterThan] ||
      f.isInstanceOf[GreaterThanOrEqual] ||
      f.isInstanceOf[LessThan] ||
      f.isInstanceOf[LessThanOrEqual]
  }


  def extractEqualConditions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filter(equalCon)
  }

  def extractRangeConditions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filter(rangeCon)
  }

  def extractResidualCondtions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filterNot(equalCon).filterNot(rangeCon)
  }
}


object RewriteMatchRule {

  def viewMatch(plan: LogicalPlan) = {
    plan transformDown {
      case a@SubqueryAlias(_, _: Project) =>
        a
      case a@SubqueryAlias(_, _) =>

        a

    }
  }

  /**
    * According to [GL01], if query sub match the view join tables,
    * You should make sure they have PK constrain. For Now, we
    * only care when the join table are fully match
    */
  def joinTableAllMatch(plan: LogicalPlan) = {

  }

}
