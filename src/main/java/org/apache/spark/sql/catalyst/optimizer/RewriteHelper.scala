package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualNullSafe, EqualTo, Exists, ExprId, Expression, ListQuery, NamedLambdaVariable, PredicateHelper, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.mlsql.sqlbooster.meta.TableHolder

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-12 WilliamZhu(allwefantasy@gmail.com)
  */
trait RewriteHelper extends PredicateHelper {

  /**
    * Since attribute references are given globally unique ids during analysis,
    * we must normalize them to check if two different queries are identical.
    */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
      case lv: NamedLambdaVariable =>
        lv.copy(exprId = ExprId(0), value = null)
    }
  }

  /**
    * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
    * equivalent:
    * 1. (a = b), (b = a);
    * 2. (a <=> b), (b <=> a).
    */
  private def rewriteEqual(condition: Expression): Expression = condition match {
    case eq@EqualTo(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
    case eq@EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }

  /**
    * Normalizes plans:
    * - Filter the filter conditions that appear in a plan. For instance,
    * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
    *   etc., will all now be equivalent.
    * - Sample the seed will replaced by 0L.
    * - Join conditions will be resorted by hashCode.
    */
  protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(_.hashCode())
          .reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteEqual).sortBy(_.hashCode())
            .reduce(And)
        Join(left, right, joinType, Some(newCondition))
    }
  }

  /** Consider symmetry for joins when comparing plans. */
  private def sameJoinPlan(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    (plan1, plan2) match {
      case (j1: Join, j2: Join) =>
        (sameJoinPlan(j1.left, j2.left) && sameJoinPlan(j1.right, j2.right)) ||
          (sameJoinPlan(j1.left, j2.right) && sameJoinPlan(j1.right, j2.left))
      case (p1: Project, p2: Project) =>
        p1.projectList == p2.projectList && sameJoinPlan(p1.child, p2.child)
      case _ =>
        plan1 == plan2
    }
  }

  def extractTablesFromPlan(plan: LogicalPlan) = {
    extractTableHolderFromPlan(plan).map { holder =>
      if (holder.db != null) holder.db + "." + holder.table
      else holder.table
    }
  }

  def extractTableHolderFromPlan(plan: LogicalPlan) = {
    var tables = Set[TableHolder]()
    plan transformDown {
      case a@SubqueryAlias(_, _) =>
        tables += TableHolder(null, a.name.unquotedString, a.output, a)
        a
      case m@HiveTableRelation(tableMeta, _, _) =>
        tables += TableHolder(tableMeta.database, tableMeta.identifier.table, m.output, m)
        m
      case m@LogicalRelation(_, output, catalogTable, _) =>
        val tableIdentifier = catalogTable.map(_.identifier)
        val database = tableIdentifier.map(_.database).flatten.getOrElse(null)
        val table = tableIdentifier.map(_.table).getOrElse(null)
        tables += TableHolder(database, table, output, m)
        m
    }
    tables.toList
  }

  /** Fails the test if the join order in the two plans do not match */
  protected def compareJoinOrder(plan1: LogicalPlan, plan2: LogicalPlan) = {
    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    sameJoinPlan(normalized1, normalized2)
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(
                              plan1: LogicalPlan,
                              plan2: LogicalPlan) = {

    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    normalized1 == normalized2
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression) = {
    comparePlans(Filter(e1, OneRowRelation()), Filter(e2, OneRowRelation()))
  }

  def mergeConjunctiveExpressions(e: Seq[Expression]) = {
    e.reduce { (a, b) =>
      And(a, b)
    }
  }

  def extractTheSameExpressions(view: Seq[Expression], query: Seq[Expression]) = {
    val viewLeft = ArrayBuffer[Expression](view: _*)
    val queryLeft = ArrayBuffer[Expression](query: _*)
    val common = ArrayBuffer[Expression]()
    query.foreach { itemInQuery =>
      view.foreach { itemInView =>
        if (itemInView.semanticEquals(itemInQuery)) {
          common += itemInQuery
          viewLeft -= itemInView
          queryLeft -= itemInQuery
        }
      }
    }
    (viewLeft, queryLeft, common)
  }

  def extractLeafExpression(expr: Expression) = {
    val columns = ArrayBuffer[AttributeReference]()
    expr transformDown {
      case a@AttributeReference(name, dataType, _, _) =>
        columns += a
        a
    }
    columns
  }
}
