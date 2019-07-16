# sql-booster

This is a library for SQL optimizing/rewriting. 
Current version (0.1.0) we have already support Materialized View rewrite.

# Liking
You can link against this library in your program at the following coordinates:

## Scala 2.11

```
groupId: tech.mlsql
artifactId: sql-booster_2.11
version: 0.1.0
```

## Usage

```scala

// register tables
ViewCatalyst.meta.registerTableFromLogicalPlan("table1", table1.logicalPlan)
ViewCatalyst.meta.registerTableFromLogicalPlan("table2", table2.logicalPlan)
ViewCatalyst.meta.registerTableFromLogicalPlan("table3", table3.logicalPlan)

// register views
/** View definition
*select table1.a,table1.b,table2.b1
 from table1
 left join table2 on table1.a=table2.b1
 left join table3 on table2.b1=table3.b2 
*/
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




