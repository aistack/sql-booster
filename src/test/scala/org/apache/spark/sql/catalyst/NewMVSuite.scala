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

  test("test join") {

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

    assert(schemaReg.genSQL(rewrite)
      == "SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)")


    val rewrite2 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT e.empid
        |FROM emps e
        |JOIN depts d
        |ON e.deptno = d.deptno
        |where e.empid=1
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite2)
      == "SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)")

    val rewrite3 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |select * from (SELECT e.empid
        |FROM emps e
        |JOIN depts d
        |ON e.deptno = d.deptno
        |where e.empid=1) as a where a.empid=2
      """.stripMargin))
    assert(schemaReg.genSQL(rewrite3)
      == "SELECT a.`empid` FROM (SELECT `empid` FROM emps_mv WHERE `empid` = CAST(1 AS BIGINT)) a WHERE a.`empid` = CAST(2 AS BIGINT)")


  }
  test("test group ") {
    schemaReg.createMV("emps_mv",
      """
        |SELECT empid, deptno
        |FROM emps
        |WHERE deptno > 5
        |GROUP BY empid, deptno
      """.stripMargin)

    val rewrite3 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT deptno
        |FROM emps
        |WHERE deptno > 10
        |GROUP BY deptno
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite3) == "SELECT `deptno` FROM emps_mv WHERE `deptno` > CAST(10 AS BIGINT) GROUP BY `deptno`")

    val rewrite4 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT deptno
        |FROM emps
        |WHERE deptno > 4
        |GROUP BY deptno
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite4) == "SELECT emps.`deptno` FROM emps WHERE emps.`deptno` > CAST(4 AS BIGINT) GROUP BY emps.`deptno`")

    val rewrite5 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT deptno
        |FROM emps
        |WHERE deptno > 5
        |GROUP BY deptno
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite5) == "SELECT `deptno` FROM emps_mv WHERE `deptno` > CAST(5 AS BIGINT) GROUP BY `deptno`")

    val rewrite6 = MaterializedViewOptimizeRewrite.execute(schemaReg.toLogicalPlan(
      """
        |SELECT deptno
        |FROM emps
        |WHERE deptno > 5
        |AND deptno <10
        |GROUP BY deptno
      """.stripMargin))

    assert(schemaReg.genSQL(rewrite6) == "SELECT `deptno` FROM emps_mv WHERE `deptno` > CAST(5 AS BIGINT) AND `deptno` < CAST(10 AS BIGINT) GROUP BY `deptno`")

  }

}
