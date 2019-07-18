package org.apache.spark.sql.catalyst

import tech.mlsql.sqlbooster.MaterializedViewOptimizeRewrite
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class NewMVSuite extends BaseSuite {

  ViewCatalyst.createViewCatalyst()

  override def beforeAll() = {
    super.init()

    schemaReg.createRDTable(
      """
        |CREATE TABLE depts(
        |  deptno INT NOT NULL,
        |  deptname VARCHAR(20),
        |  PRIMARY KEY (deptno)
        |);
      """.stripMargin)

    schemaReg.createRDTable(
      """
        |CREATE TABLE locations(
        |  locationid INT NOT NULL,
        |  state CHAR(2),
        |  PRIMARY KEY (locationid)
        |);
      """.stripMargin)

    schemaReg.createRDTable(
      """
        |CREATE TABLE emps(
        |  empid INT NOT NULL,
        |  deptno INT NOT NULL,
        |  locationid INT NOT NULL,
        |  empname VARCHAR(20) NOT NULL,
        |  salary DECIMAL (18, 2),
        |  PRIMARY KEY (empid),
        |  FOREIGN KEY (deptno) REFERENCES depts(deptno),
        |  FOREIGN KEY (locationid) REFERENCES locations(locationid)
        |);
      """.stripMargin)

    schemaReg.createHiveTable("src",
      """
        |CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive
      """.stripMargin)

  }

  test("test 1") {

    schemaReg.createMV("emps_mv",
      """
        |SELECT empid
        |FROM emps
        |JOIN depts ON depts.deptno = emps.deptno
      """.stripMargin)

    val rewrite = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT empid
        |FROM emps
        |JOIN depts
        |ON depts.deptno = emps.deptno
        |where emps.empid=1
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite) == "SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)")


    val rewrite2 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT e.empid
        |FROM emps e
        |JOIN depts d
        |ON e.deptno = d.deptno
        |where e.empid=1
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite2) == "SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)")


  }
  test("test 2") {

    schemaReg.createMV("emps_mv",
      """
        |SELECT empid
        |FROM emps
        |JOIN depts ON depts.deptno = emps.deptno
      """.stripMargin)

    val rewrite3 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |select * from (SELECT e.empid
        |FROM emps e
        |JOIN depts d
        |ON e.deptno = d.deptno
        |where e.empid=1) as a where a.empid=2
      """.stripMargin))
    println(schemaReg.genSQL(rewrite3) == "SELECT a.`empid` FROM (SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)) a WHERE a.`empid` = CAST(2 AS BIGINT)")
    
  }

}
