package org.apache.spark.sql.catalyst

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{DebugFilesystem, SparkConf}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import tech.mlsql.sqlbooster.SchemaRegistry

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
class BaseSuite extends FunSuite
  with BeforeAndAfterAll with PredicateHelper {
  var spark: SparkSession = _
  var schemaReg: SchemaRegistry = null

  def init(): Unit = {
    FileUtils.deleteDirectory(new File("./metastore_db"))
    FileUtils.deleteDirectory(new File("/tmp/spark-warehouse"))
    spark = SparkSession.builder().
      config(sparkConf).
      master("local[*]").
      appName("base-test").
      enableHiveSupport().getOrCreate()
    schemaReg = new SchemaRegistry(spark)
  }

  def prepareDefaultTables = {
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

  override def afterAll(): Unit = {
    //SparkSession.cleanupAnyExistingSession()
    spark.close()
  }

  def sparkConf = {
    new SparkConf()
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)
      .set(CATALOG_IMPLEMENTATION.key, "hive")
      .set("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
  }


}
