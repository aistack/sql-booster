# sql-booster

A library for optimizing/rewriting SQL. For now, it supports Materialized View.

## Usage

```scala

// register tables
ViewCatalyst.meta.registerTableFromLogicalPlan("table1", table1.logicalPlan)
ViewCatalyst.meta.registerTableFromLogicalPlan("table2", table2.logicalPlan)
ViewCatalyst.meta.registerTableFromLogicalPlan("table3", table3.logicalPlan)

// register views
ViewCatalyst.meta.registerMaterializedViewFromLogicalPlan("viewTable1", viewTable1.logicalPlan, createViewTable1.logicalPlan)

val analyzed3 = spark.sql(
        """
          |select table1.a
          |from table1
          |left join table2 on table1.a=table2.b1
          |left join table3 on table2.b1=table3.b2
          |where table2.b1=2
        """.stripMargin).queryExecution.analyzed
        
val rewrite = MaterializedViewOptimizeRewrite.execute(analyzed3)
println(new LogicalPlanSQL(rewrite, new BasicSQLDialect).toSQL)
// result: SELECT `a` FROM viewTable1 WHERE 2 = `b1`

        
```

Here is the rewrite SQL

```sql
SELECT `a` FROM viewTable1 WHERE 2 = `b1`
```


