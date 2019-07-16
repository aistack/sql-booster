package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

object PreOptimizeRewrite extends RuleExecutor[LogicalPlan] {
  val batches =
    Batch("Before join rewrite", FixedPoint(100),
      EliminateOuterJoin, PushPredicateThroughJoin) :: Nil
}
