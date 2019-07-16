package org.apache.spark.sql.catalyst

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.sqlgenerator.{BasicSQLDialect, LogicalPlanSQL}
import org.apache.spark.sql.streaming.StreamTest
import tech.mlsql.sqlbooster.MaterializedViewOptimizeRewrite
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
class MVSuite extends StreamTest with PredicateHelper {


  test("join test") {

    createView("viewTable1",
      """
        |select table1.a,table1.b,table2.b1
        |from table1
        |left join table2 on table1.a=table2.b1
        |left join table3 on table2.b1=table3.b2
      """.stripMargin)

    var rewrite = MaterializedViewOptimizeRewrite.execute(lp(
      """
        |select t1.a
        |from table1  t1
        |left join table2 t2 on t1.a=t2.b1
        |left join table3 t3 on t2.b1=t3.b2
        |where t2.b1=2
      """.stripMargin))
    assert(g(rewrite) =="""SELECT `a` FROM viewTable1 WHERE 2 = `b1`""")

    rewrite = MaterializedViewOptimizeRewrite.execute(lp(
      """
        |select table1.a
        |from table1
        |left join table2 t2 on table1.a=t2.b1
        |left join table3 t3 on t2.b1=t3.b2
        |where t2.b1=2
      """.stripMargin))
    assert(g(rewrite) =="""SELECT `a` FROM viewTable1 WHERE 2 = `b1`""")

  }

  test("where test") {

    createView("viewTable2",
      """
        |select * from table1 where a=1
      """.stripMargin)

    var rewrite = MaterializedViewOptimizeRewrite.execute(lp(
      """
        |select a,b from table1 where a=1 and b=2
      """.stripMargin))
    assert(g(rewrite) =="""SELECT `a`,`b` FROM viewTable2 WHERE 2 = `b`""")

  }


  test("group by") {

    createView("viewTable3",
      """
        |select a,b,count(*) as total,sum(b) as wow from table1 where a=1 group by a,b
      """.stripMargin)

    var rewrite = MaterializedViewOptimizeRewrite.execute(lp(
      """
        |select b,count(*) as jack,sum(b) as wow1 from table1 where a=1 group by b
      """.stripMargin))
    assert(g(rewrite) =="""SELECT `b`, sum(`total`) AS `jack`, `wow` AS `wow1` FROM viewTable3 GROUP BY `b`""")

  }
  

  def lp(sql: String) = {
    val temp = spark.sql(sql).queryExecution.analyzed
    println(s"original:\n${temp}")
    temp
  }

  def g(lp: LogicalPlan) = {
    println(s"rewrite:\n${lp}")
    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
    println(s"sql:${temp}")
    temp
  }

  def createView(viewName: String, viewCreate: String) = {
    spark.sql(viewCreate).write.mode(SaveMode.Overwrite).parquet(s"/tmp/${viewName}")
    val viewTable1 = spark.read.parquet(s"/tmp/${viewName}").select("*")
    val createViewTable1 = spark.sql(viewCreate)

    ViewCatalyst.meta.registerMaterializedViewFromLogicalPlan(viewName, viewTable1.logicalPlan, createViewTable1.logicalPlan)
  }

  def prepareTableAndView = {

    spark.sql("select 1 as a, 2 as b,3 as c").write.mode(SaveMode.Overwrite).parquet("/tmp/table1")
    spark.sql("select 1 as a1, 2 as b1,3 as c1").write.mode(SaveMode.Overwrite).parquet("/tmp/table2")
    spark.sql("select 1 as a2, 2 as b2,3 as c2").write.mode(SaveMode.Overwrite).parquet("/tmp/table3")

    val table1 = spark.read.parquet("/tmp/table1")
    table1.createOrReplaceTempView("table1")
    val table2 = spark.read.parquet("/tmp/table2")
    table2.createOrReplaceTempView("table2")

    val table3 = spark.read.parquet("/tmp/table3")
    table3.createOrReplaceTempView("table3")

    ViewCatalyst.meta.registerTableFromLogicalPlan("table1", table1.logicalPlan)
    ViewCatalyst.meta.registerTableFromLogicalPlan("table2", table2.logicalPlan)
    ViewCatalyst.meta.registerTableFromLogicalPlan("table3", table3.logicalPlan)

  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    ViewCatalyst.createViewCatalyst()
    prepareTableAndView
  }

}
