package org.apache.spark.sql.catalyst

import net.liftweb.{json => SJSon}
import tech.mlsql.sqlbooster.DataLineageExtractor
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-19 WilliamZhu(allwefantasy@gmail.com)
  */
class DataLineageSuite extends BaseSuite {
  ViewCatalyst.createViewCatalyst()

  test("data lineage test") {
    val result = DataLineageExtractor.execute(schemaReg.toLogicalPlan(
      """
        |select * from (SELECT e.empid
        |FROM emps e
        |JOIN depts d
        |ON e.deptno = d.deptno
        |where e.empid=1) as a where a.empid=2
      """.stripMargin))
    println(JSONTool.pretty(result))
  }

  override def beforeAll() = {
    super.init()
    super.prepareDefaultTables
  }

}

object JSONTool {

  def pretty(item: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.writePretty(item)
  }

  def parseJson[T](str: String)(implicit m: Manifest[T]) = {
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(str).extract[T]
  }

  def toJsonStr(item: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(item)
  }

}
