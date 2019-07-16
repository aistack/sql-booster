package tech.mlsql.sqlbooster

import org.apache.spark.sql.catalyst.optimizer.RewriteTableToViews
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

/**
  * 2019-07-16 WilliamZhu(allwefantasy@gmail.com)
  */
object MaterializedViewOptimizeRewrite extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Materialized view rewrite", Once,
      RewriteTableToViews) :: Nil
}