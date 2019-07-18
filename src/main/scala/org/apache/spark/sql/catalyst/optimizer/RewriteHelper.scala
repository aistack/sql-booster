package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualNullSafe, EqualTo, Exists, ExprId, Expression, ListQuery, NamedLambdaVariable, PredicateHelper, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{ProcessedComponent, RewriteContext}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
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
      Seq(l, r).sortBy(hashCode).reduce(EqualTo)
    case eq@EqualNullSafe(l: Expression, r: Expression) =>
      Seq(l, r).sortBy(hashCode).reduce(EqualNullSafe)
    case _ => condition // Don't reorder.
  }

  def hashCode(_ar: Expression): Int = {
    // See http://stackoverflow.com/questions/113511/hash-code-implementation
    _ar match {
      case ar@AttributeReference(_, _, _, _) =>
        var h = 17
        h = h * 37 + ar.name.hashCode()
        h = h * 37 + ar.dataType.hashCode()
        h = h * 37 + ar.nullable.hashCode()
        h = h * 37 + ar.metadata.hashCode()
        h = h * 37 + ar.exprId.hashCode()
        h
      case _ => _ar.hashCode()
    }

  }

  /**
    * Normalizes plans:
    * - Filter the filter conditions that appear in a plan. For instance,
    * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
    *   etc., will all now be equivalent.
    * - Sample the seed will replaced by 0L.
    * - Join conditions will be resorted by hashCode.
    *
    * we use new hash function to avoid `ar.qualifier` from alias affect the final order.
    *
    */
  protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {


    plan transform {
      case Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).map(rewriteEqual).sortBy(hashCode)
          .reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)
      case Join(left, right, joinType, condition) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteEqual).sortBy(hashCode)
            .reduce(And)
        Join(left, right, joinType, Some(newCondition))
    }
  }

  /** Consider symmetry for joins when comparing plans. */
  def sameJoinPlan(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
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
    }.filterNot(f => f == null)
  }

  def extractTableHolderFromPlan(plan: LogicalPlan) = {
    var tables = Set[TableHolder]()
    plan transformDown {
      case a@SubqueryAlias(_, LogicalRelation(_, _, _, _)) =>
        tables += TableHolder(null, a.name.unquotedString, a.output, a)
        a
      case a@SubqueryAlias(_, LogicalRDD(_, _, _, _, _)) =>
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


  def extractAttributeReference(expr: Expression) = {
    val columns = ArrayBuffer[AttributeReference]()
    expr transformDown {
      case a@AttributeReference(name, dataType, _, _) =>
        columns += a
        a
    }
    columns
  }

  def extractAttributeReferenceFromFirstLevel(exprs: Seq[Expression]) = {
    exprs.map { expr =>
      expr match {
        case a@AttributeReference(name, dataType, _, _) => Option(a)
        case _ => None
      }
    }.filter(_.isDefined).map(_.get)
  }

  /**
    * Sometimes we compare two tables with column name and dataType
    */
  def attributeReferenceEqual(a: AttributeReference, b: AttributeReference) = {
    a.name == b.name && a.dataType == b.dataType
  }

  def isJoinExists(plan: LogicalPlan) = {
    var _isJoinExists = false
    plan transformDown {
      case a@Join(_, _, _, _) =>
        _isJoinExists = true
        a
    }
    _isJoinExists
  }

  def isAggExistsExists(plan: LogicalPlan) = {
    var _isAggExistsExists = false
    plan transformDown {
      case a@Aggregate(_, _, _) =>
        _isAggExistsExists = true
        a
    }
    _isAggExistsExists
  }

  def generateRewriteContext(plan: LogicalPlan, rewriteContext: RewriteContext) = {
    var queryConjunctivePredicates: Seq[Expression] = Seq()
    var viewConjunctivePredicates: Seq[Expression] = Seq()

    var queryProjectList: Seq[Expression] = Seq()
    var viewProjectList: Seq[Expression] = Seq()

    var queryGroupingExpressions: Seq[Expression] = Seq()
    var viewGroupingExpressions: Seq[Expression] = Seq()

    var queryAggregateExpressions: Seq[Expression] = Seq()
    var viewAggregateExpressions: Seq[Expression] = Seq()

    val viewJoins = ArrayBuffer[Join]()
    val queryJoins = ArrayBuffer[Join]()

    val queryNormalizePlan = normalizePlan(plan)
    val viewNormalizePlan = normalizePlan(rewriteContext.viewLogicalPlan.get.viewCreateLogicalPlan)
    //collect all predicates
    viewNormalizePlan transformDown {
      case a@Filter(condition, _) =>
        viewConjunctivePredicates ++= splitConjunctivePredicates(condition)
        a
    }

    queryNormalizePlan transformDown {
      case a@Filter(condition, _) =>
        queryConjunctivePredicates ++= splitConjunctivePredicates(condition)
        a
    }

    // check projectList and where condition
    normalizePlan(plan) match {
      case Project(projectList, Filter(condition, _)) =>
        queryConjunctivePredicates = splitConjunctivePredicates(condition)
        queryProjectList = projectList
      case Project(projectList, _) =>
        queryProjectList = projectList

      case Aggregate(groupingExpressions, aggregateExpressions, Filter(condition, _)) => {
        queryConjunctivePredicates = splitConjunctivePredicates(condition)
        queryGroupingExpressions = groupingExpressions
        queryAggregateExpressions = aggregateExpressions
      }
      case Aggregate(groupingExpressions, aggregateExpressions, _) =>
        queryGroupingExpressions = groupingExpressions
        queryAggregateExpressions = aggregateExpressions


    }

    normalizePlan(rewriteContext.viewLogicalPlan.get().viewCreateLogicalPlan) match {
      case Project(projectList, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewProjectList = projectList
      case Project(projectList, _) =>
        viewProjectList = projectList

      case Aggregate(groupingExpressions, aggregateExpressions, Filter(condition, _)) =>
        viewConjunctivePredicates = splitConjunctivePredicates(condition)
        viewGroupingExpressions = groupingExpressions
        viewAggregateExpressions = aggregateExpressions

      case Aggregate(groupingExpressions, aggregateExpressions, _) =>
        viewGroupingExpressions = groupingExpressions
        viewAggregateExpressions = aggregateExpressions
    }

    if (isJoinExists(plan)) {
      // get the first level join
      viewJoins += extractFirstLevelJoin(viewNormalizePlan)
      queryJoins += extractFirstLevelJoin(queryNormalizePlan)
    }


    rewriteContext.processedComponent.set(ProcessedComponent(
      queryConjunctivePredicates,
      viewConjunctivePredicates,
      queryProjectList,
      viewProjectList,
      queryGroupingExpressions,
      viewGroupingExpressions,
      queryAggregateExpressions,
      viewAggregateExpressions,
      viewJoins,
      queryJoins
    ))

  }

  def extractFirstLevelJoin(plan: LogicalPlan) = {
    plan match {
      case p@Project(_, join@Join(_, _, _, _)) => join
      case p@Project(_, Filter(_, join@Join(_, _, _, _))) => join
      case p@Aggregate(_, _, Filter(_, join@Join(_, _, _, _))) => join
      case p@Aggregate(_, _, join@Join(_, _, _, _)) => join
    }
  }

  

}
