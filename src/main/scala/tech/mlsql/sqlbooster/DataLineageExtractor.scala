package tech.mlsql.sqlbooster

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.RewritedLeafLogicalPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import tech.mlsql.sqlbooster.analysis._

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-19 WilliamZhu(allwefantasy@gmail.com)
  */
object DataLineageExtractor extends RewriteHelper {
  def execute(plan: LogicalPlan): DataLineage = {


    // collect tables
    val tables = extractTableHolderFromPlan(plan)

    def findDependencesFromColumns(columns: Seq[Expression]) = {
      val tempHolder = ArrayBuffer[TableAndUsedColumns]()
      tables.foreach { table =>
        val tableAndUsedColumns = TableAndUsedColumns(table.table, Seq(), Seq())
        val tableOutput = table.output
        val tempItems = columns.flatMap { atr =>
          tableOutput.filter(f => f.semanticEquals(atr)).toSet
        }
        val tempColumns = tempItems.map(f => (f.name, Location.locate(plan, f))).groupBy(_._1).map { ar =>
          (ar._1, ar._2.flatMap(f => f._2).toSet.toSeq)
        }
        tempHolder += tableAndUsedColumns.copy(
          columns = tempColumns.map(_._1).toSeq,
          locates = tempColumns.map(_._2).toSeq
        )
      }
      tempHolder
    }

    val arBuffer = ArrayBuffer[AttributeReference]()

    // collect all attributeRef without original tables

    val newPlan = plan transformDown {
      case a@SubqueryAlias(_, LogicalRelation(_, _, _, _)) =>
        RewritedLeafLogicalPlan(null)
      case a@SubqueryAlias(_, LogicalRDD(_, _, _, _, _)) =>
        RewritedLeafLogicalPlan(null)
      case a@SubqueryAlias(_, m@HiveTableRelation(tableMeta, _, _)) =>
        RewritedLeafLogicalPlan(null)
      case m@HiveTableRelation(tableMeta, _, _) =>
        RewritedLeafLogicalPlan(null)
      case m@LogicalRelation(_, output, catalogTable, _) =>
        RewritedLeafLogicalPlan(null)
    }

    newPlan.transformAllExpressions {
      case a@AttributeReference(_, _, _, _) =>
        arBuffer += a
        a
    }
    val dependences = findDependencesFromColumns(arBuffer)


    val outputMapToSourceTable = plan.output.map { case columnItem =>
      val arBuffer = ArrayBuffer[AttributeReference]()
      columnItem.transformDown {
        case a@AttributeReference(_, _, _, _) =>
          arBuffer += a
          a
      }
      OutputColumnToSourceTableAndColumn(columnItem.name, findDependencesFromColumns(arBuffer))
    }

    DataLineage(outputMapToSourceTable, dependences)
  }
}


