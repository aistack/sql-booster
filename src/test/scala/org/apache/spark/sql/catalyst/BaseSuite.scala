package org.apache.spark.sql.catalyst

import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.sqlgenerator.{BasicSQLDialect, LogicalPlanSQL}
import org.apache.spark.sql.streaming.StreamTest
import tech.mlsql.sqlbooster.db.RDSchema
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class BaseSuite extends StreamTest with PredicateHelper {
  def createTable(createSQL: String) = {
    val rd = new RDSchema(JdbcConstants.MYSQL)
    val tableName = rd.createTable(createSQL)
    val schema = rd.getTableSchema(tableName)
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.createOrReplaceTempView(tableName)
    ViewCatalyst.meta.registerTableFromLogicalPlan(tableName, df.logicalPlan)
  }

  def createMV(viewName: String, viewCreate: String) = {
    val createViewTable1 = spark.sql(viewCreate)
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], createViewTable1.schema)
    df.createOrReplaceTempView(viewName)
    ViewCatalyst.meta.registerMaterializedViewFromLogicalPlan(viewName, df.logicalPlan, createViewTable1.logicalPlan)
  }

  def toLogicalPlan(sql: String) = {
    val temp = spark.sql(sql).queryExecution.analyzed
    println(s"original:\n${temp}")
    temp
  }

  def genSQL(lp: LogicalPlan) = {
    println(s"rewrite:\n${lp}")
    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
    println(s"sql:${temp}")
    temp
  }
}
