package tech.mlsql.sqlbooster.meta

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

import scala.collection.JavaConverters._

/**
  * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
  */

class ViewCatalyst(sparkOpt: Option[SparkSession]) extends RewriteHelper {

  //view name -> LogicalPlan
  private val data = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  //table -> view
  private val tableToViews = new java.util.concurrent.ConcurrentHashMap[String, Set[String]]()


  /**
    *
    * @param name              the View Name
    * @param sql               the Select SQL which create the view.
    * @param _createSchemaList format like List("tableName=st(field(a,string),(b,string)")
    * @return
    */
  def register(name: String, sql: String, _createSchemaList: List[String] = List()) = {
    val spark = sparkOpt.get

    def pushToTableToViews(tableName: String) = {
      val items = tableToViews.asScala.getOrElse(tableName, Set[String]())
      tableToViews.put(tableName, items ++ Set(name))
    }

    _createSchemaList.foreach { tableAndSchema =>
      val Array(tableName, schema) = tableAndSchema.split("=")
      val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SparkSimpleSchemaParser.parse(schema).asInstanceOf[StructType])
      df.createOrReplaceTempView(tableName)
      pushToTableToViews(tableName)
    }
    val df = spark.sql(sql)

    if (_createSchemaList.size == 0) {
      extractTablesFromPlan(df.queryExecution.analyzed).foreach { tableName =>
        pushToTableToViews(tableName)
      }
    }

    val lp = df.queryExecution.analyzed
    data.put(name, lp)
    this

  }


  def getCandinateViewsByTable(tableName: String) = {
    tableToViews.asScala.get(tableName)
  }

  def getViewLogicalPlan(viewName: String) = {
    data.asScala.get(viewName)
  }
}

case class TableHolder(db: String, table: String, output: Seq[NamedExpression], lp: LogicalPlan)

object ViewCatalyst {
  private var _meta: ViewCatalyst = null

  def createViewCatalyst(spark: Option[SparkSession] = None) = {
    _meta = new ViewCatalyst(spark)
  }

  def meta = {
    if (_meta == null) throw new RuntimeException("ViewCatalyst is not initialed. Please invoke createViewCatalyst before call this function.")
    _meta
  }
}
