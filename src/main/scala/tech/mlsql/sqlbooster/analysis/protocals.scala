package tech.mlsql.sqlbooster.analysis

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}

import scala.collection.mutable.ArrayBuffer

case class DataLineage(outputMapToSourceTable: Seq[OutputColumnToSourceTableAndColumn], dependences: Seq[TableAndUsedColumns])

case class TableAndUsedColumns(tableName: String, columns: Seq[String], locates: Seq[Seq[String]])

case class OutputColumnToSourceTableAndColumn(name: String, sources: Seq[TableAndUsedColumns])



object Location {
  val FILTER = "FILTER"
  val GROUP_BY = "GROUP_BY"
  val JOIN = "JOIN"
  val PROJECT = "PROJECT"

  def locate(plan: LogicalPlan, exp: Expression) = {
    val locates = ArrayBuffer[String]()
    plan transformDown {
      case a@Filter(condition, child) =>
        if (existsIn(exp, Seq(condition))) {
          locates += FILTER
        }
        a
      case a@Join(_, _, _, condition) =>
        if (condition.isDefined && existsIn(exp, Seq(condition.get))) {
          locates += JOIN
        }
        a
      case a@Aggregate(groupingExpressions, aggregateExpressions, _) =>
        if (existsIn(exp, groupingExpressions)) {
          locates += GROUP_BY
        }
        if (existsIn(exp, aggregateExpressions)) {
          locates += PROJECT
        }
        a
    }
    locates.toSet.toSeq
  }

  def existsIn(exp: Expression, targetExpr: Seq[Expression]) = {
    var exists = false
    targetExpr.map { item =>
      item transformDown {
        case ar@AttributeReference(_, _, _, _) =>
          if (ar.semanticEquals(exp)) {
            exists = true
          }
          ar
      }
    }
    exists
  }
}
