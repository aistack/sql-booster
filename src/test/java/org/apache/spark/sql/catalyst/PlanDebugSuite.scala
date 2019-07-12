package org.apache.spark.sql.catalyst


import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{QueryTest, Row}
import tech.mlsql.schema.parser.SparkSimpleSchemaParser

/**
  * 2019-07-11 WilliamZhu(allwefantasy@gmail.com)
  */
class PlanDebugSuite extends PlanTest with PredicateHelper {
  val testRelation0 = LocalRelation('a.int, 'b.int, 'c.int).as("t0")
  val testRelation1 = LocalRelation('d.int).as("t1")
  val testRelation2 = LocalRelation('b.int, 'c.int, 'e.int).as("t2")

  test("join only") {
    val left = testRelation0.where('a === 1)
    val right = testRelation1
    val originalQuery =
      left.join(right, condition = Some("t0.d".attr === "t1.b".attr || "t0.d".attr === "t1.c".attr)).join(
        testRelation2, condition = Some("t0.d".attr === "t2.c".attr)).analyze
    println(originalQuery)
    val m = testRelation0.where("t0.d".attr === "t1.b".attr || "t0.d".attr === "t1.c".attr)
    m match {
      case Filter(condition, _) =>
        condition.toString()
        println(splitConjunctivePredicates(condition))
    }
  }

  test("where wow") {
    val wow = testRelation0.where(("t0.a".attr === 1 && ("t0.a".attr > 0 || ("t0.a".attr < 4 && "t0.a".attr > 2))) && ("t0.b".attr === 3))
    normalizePlan(wow) match {
      case Filter(condition, _) =>
        println(splitConjunctivePredicates(condition))
    }

  }


}

class DFDebugSuite extends QueryTest with SharedSQLContext {
  def createTable(name: String, schema: String) = {
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], SparkSimpleSchemaParser.parse(schema).asInstanceOf[StructType])
    df.createOrReplaceTempView(name)
  }

  test("test") {
    createTable("at", "st(field(a,string),field(b,string))")
    createTable("bt", "st(field(a1,string),field(b1,string))")

    val df = spark.sql("select * from (select at.b,count(bt.b1) as c from at left join bt on at.a == bt.a1 where at.a='yes' group by at.b) m")
    println(df.queryExecution.analyzed)

    println(spark.sql("select * from at where at.a='yes' order by at.b desc").queryExecution.analyzed)
    println(spark.sql("select * from at,bt where at.a=bt.a1 and (at.b=bt.b1 or at.a=bt.b1)").queryExecution.analyzed)

  }
}




