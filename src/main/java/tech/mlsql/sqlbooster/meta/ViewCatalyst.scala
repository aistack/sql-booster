package tech.mlsql.sqlbooster.meta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.JavaConverters._

/**
  * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
  */

class ViewCatalyst extends RewriteHelper {

  //view name -> LogicalPlan
  private val viewToCreateLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  //view name -> LogicalPlan
  private val viewToLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  //table -> view
  private val tableToViews = new java.util.concurrent.ConcurrentHashMap[String, Set[String]]()


  def registerFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan, createLP: LogicalPlan) = {

    def pushToTableToViews(tableName: String) = {
      val items = tableToViews.asScala.getOrElse(tableName, Set[String]())
      tableToViews.put(tableName, items ++ Set(name))
    }

    extractTablesFromPlan(createLP).foreach { tableName =>
      pushToTableToViews(tableName)
    }

    viewToCreateLogicalPlan.put(name, createLP)
    viewToLogicalPlan.put(name, tableLogicalPlan)
    this

  }


  def getCandinateViewsByTable(tableName: String) = {
    tableToViews.asScala.get(tableName)
  }

  def getViewLogicalPlan(viewName: String) = {
    viewToLogicalPlan.asScala.get(viewName)
  }

  def getViewCreateLogicalPlan(viewName: String) = {
    viewToCreateLogicalPlan.asScala.get(viewName)
  }
}

case class TableHolder(db: String, table: String, output: Seq[NamedExpression], lp: LogicalPlan)

object ViewCatalyst {
  private var _meta: ViewCatalyst = null

  def createViewCatalyst(spark: Option[SparkSession] = None) = {
    _meta = new ViewCatalyst()
  }

  def meta = {
    if (_meta == null) throw new RuntimeException("ViewCatalyst is not initialed. Please invoke createViewCatalyst before call this function.")
    _meta
  }
}
