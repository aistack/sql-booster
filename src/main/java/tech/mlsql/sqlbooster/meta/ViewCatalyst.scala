package tech.mlsql.sqlbooster.meta

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.JavaConverters._

/**
  * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
  */

trait ViewCatalyst {
  def registerMaterializedViewFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan, createLP: LogicalPlan): ViewCatalyst

  def registerTableFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan): ViewCatalyst

  def getCandidateViewsByTable(tableName: String): Option[Set[String]]

  def getViewLogicalPlan(viewName: String): Option[LogicalPlan]

  def getViewCreateLogicalPlan(viewName: String): Option[LogicalPlan]

  def getViewNameByLogicalPlan(viewLP: LogicalPlan): Option[String]

  def getTableNameByLogicalPlan(viewLP: LogicalPlan): Option[String]

}

class SimpleViewCatalyst extends ViewCatalyst with RewriteHelper {

  //view name -> LogicalPlan
  private val viewToCreateLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  //view name -> LogicalPlan
  private val viewToLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  //table -> view
  private val tableToViews = new java.util.concurrent.ConcurrentHashMap[String, Set[String]]()

  // simple meta data for LogicalPlanSQL
  private val logicalPlanToTableName = new java.util.concurrent.ConcurrentHashMap[LogicalPlan, String]()


  override def registerMaterializedViewFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan, createLP: LogicalPlan) = {

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

  override def registerTableFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan) = {
    logicalPlanToTableName.put(tableLogicalPlan, name)
    this

  }


  override def getCandidateViewsByTable(tableName: String) = {
    tableToViews.asScala.get(tableName)
  }

  override def getViewLogicalPlan(viewName: String) = {
    viewToLogicalPlan.asScala.get(viewName)
  }

  override def getViewCreateLogicalPlan(viewName: String) = {
    viewToCreateLogicalPlan.asScala.get(viewName)
  }

  override def getViewNameByLogicalPlan(viewLP: LogicalPlan) = {
    viewToLogicalPlan.asScala.filter(f => f._2 == viewLP).map(f => f._1).headOption
  }

  override def getTableNameByLogicalPlan(viewLP: LogicalPlan) = {
    logicalPlanToTableName.asScala.get(viewLP)
  }
}

case class TableHolder(db: String, table: String, output: Seq[NamedExpression], lp: LogicalPlan)

object ViewCatalyst {
  private var _meta: ViewCatalyst = null

  def createViewCatalyst(clzz: Option[String] = None) = {
    _meta = if (clzz.isDefined) Class.forName(clzz.get).newInstance().asInstanceOf[ViewCatalyst] else new SimpleViewCatalyst()
  }

  def meta = {
    if (_meta == null) throw new RuntimeException("ViewCatalyst is not initialed. Please invoke createViewCatalyst before call this function.")
    _meta
  }
}
