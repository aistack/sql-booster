package tech.mlsql.sqlbooster

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.catalyst.SessionUtil
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.sqlgenerator.{BasicSQLDialect, LogicalPlanSQL}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser
import tech.mlsql.sqlbooster.db.RDSchema
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class SchemaRegistry(_spark: SparkSession) {
  val spark = SessionUtil.cloneSession(_spark)

  def createRDTable(createSQL: String) = {
    val rd = new RDSchema(JdbcConstants.MYSQL)
    val tableName = rd.createTable(createSQL)
    val schema = rd.getTableSchema(tableName)
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.createOrReplaceTempView(tableName)
    ViewCatalyst.meta.registerTableFromLogicalPlan(tableName, df.queryExecution.analyzed)
  }

  def createHiveTable(tableName: String, createSQL: String) = {
    spark.sql(createSQL)
    val lp = spark.table(tableName).queryExecution.analyzed match {
      case a@SubqueryAlias(name, child) => child
      case a@_ => a
    }
    ViewCatalyst.meta.registerTableFromLogicalPlan(tableName, lp)
  }

  def createSPTable(tableName: String, schemaText: String) = {
    val schema = SparkSimpleSchemaParser.parse(schemaText).asInstanceOf[StructType]
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.createOrReplaceTempView(tableName)
    ViewCatalyst.meta.registerTableFromLogicalPlan(tableName, df.queryExecution.analyzed)
  }

  def createMV(viewName: String, viewCreate: String) = {
    val createViewTable1 = spark.sql(viewCreate)
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], createViewTable1.schema)
    df.createOrReplaceTempView(viewName)
    ViewCatalyst.meta.registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
    ViewCatalyst.meta.registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.analyzed, createViewTable1.queryExecution.analyzed)
  }

  def toLogicalPlan(sql: String) = {
    val temp = spark.sql(sql).queryExecution.analyzed
    temp
  }

  def genSQL(lp: LogicalPlan) = {
    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
    temp
  }

  def genPrettySQL(lp: LogicalPlan) = {
    SQLUtils.format(genSQL(lp), JdbcConstants.HIVE)
  }
}
