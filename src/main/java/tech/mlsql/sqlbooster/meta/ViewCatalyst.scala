package tech.mlsql.sqlbooster.meta

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
  */

class ViewCatalyst(spark: SparkSession) {

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
    def extractAllRefTables(df: DataFrame) = {
      val tables = ArrayBuffer[String]()
      df.queryExecution.analyzed.transformDown {
        case a@SubqueryAlias(_, _: Project) =>
          a
        case a@SubqueryAlias(_, _) =>
          tables += a.name.unquotedString
          a

      }
      tables
    }

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
      extractAllRefTables(df).foreach { tableName =>
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

object ViewCatalyst {
  private var _meta: ViewCatalyst = null

  def createViewCatalyst(spark: SparkSession) = {
    _meta = new ViewCatalyst(spark)
  }

  def meta = {
    if (_meta == null) throw new RuntimeException("ViewCatalyst is not initialed. Please invoke createViewCatalyst before call this function.")
    _meta
  }
}
