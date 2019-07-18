package org.apache.spark.sql.catalyst

import tech.mlsql.sqlbooster.MaterializedViewOptimizeRewrite
import tech.mlsql.sqlbooster.meta.ViewCatalyst

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class NewMVSuite extends BaseSuite {

  ViewCatalyst.createViewCatalyst()

  test("test 1") {
    createTable(
      """
        |CREATE TABLE depts(
        |  deptno INT NOT NULL,
        |  deptname VARCHAR(20),
        |  PRIMARY KEY (deptno)
        |);
      """.stripMargin)

    createTable(
      """
        |CREATE TABLE locations(
        |  locationid INT NOT NULL,
        |  state CHAR(2),
        |  PRIMARY KEY (locationid)
        |);
      """.stripMargin)

    createTable(
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

    createMV("emps_mv",
      """
        |SELECT empid
        |FROM emps
        |JOIN depts ON depts.deptno = emps.deptno
      """.stripMargin)

    val rewrite = MaterializedViewOptimizeRewrite.execute(toLogicalPlan(
      """
        |SELECT empid
        |FROM emps
        |JOIN depts
        |ON depts.deptno = emps.deptno
        |where emps.empid=1
      """.stripMargin))

    genSQL(rewrite)

  }
}
