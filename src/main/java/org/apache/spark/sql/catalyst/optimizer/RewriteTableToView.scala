package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.WithoutJoinGroupRule
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

import scala.collection.mutable.ArrayBuffer

/**
  * References:
  * [GL01] Jonathan Goldstein and Per-åke Larson.
  * Optimizing queries using materialized views: A practical, scalable solution. In Proc. ACM SIGMOD Conf., 2001.
  *
  * The Rule should be used on the resolved analyzer, for
  * example:
  * {{{
  *       object OptimizeRewrite extends RuleExecutor[LogicalPlan] {
  *               val batches =
  *                        Batch("User Rewriter", Once,
  *                       RewriteTableToViews) :: Nil
  *       }
  *
  *       ViewCatalyst.createViewCatalyst(Option(spark))
  *       ViewCatalyst.meta.register("ct", """ select * from at where a="jack" """)
  *
  *       val analyzed = spark.sql(""" select * from at where a="jack" and b="wow" """).queryExecution.analyzed
  *       val mvRewrite = OptimizeRewrite.execute(analyzed)
  *
  *       // do other stuff to mvRewrite
  * }}}
  *
  *
  */
object RewriteTableToViews extends Rule[LogicalPlan] with PredicateHelper {
  val batches = ArrayBuffer[WithoutJoinGroupRule](
    WithoutJoinGroupRule.apply
  )

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (isSPJG(plan)) {
      rewrite(plan)
    } else {
      plan.transformDown {
        case a if isSPJG(a) => rewrite(a)
      }
    }

  }

  private def rewrite(plan: LogicalPlan) = {
    // this plan is SPJG, but the first step is check whether we can rewrite it
    var rewritePlan = plan
    batches.foreach { rewriter =>
      rewritePlan = rewriter.rewrite(rewritePlan)
    }
    rewritePlan
  }

  /**
    * check the plan is whether a basic sql pattern
    * only contains select(filter)/agg/project/join/group.
    *
    * @param plan
    * @return
    */
  private def isSPJG(plan: LogicalPlan) = {
    plan match {

      case p@Project(_, Join(_, _, _, _)) => true
      case p@Project(_, Filter(_, Join(_, _, _, _))) => true
      case p@Aggregate(_, _, Filter(_, Join(_, _, _, _))) => true
      case p@Project(_, Filter(_, _)) => true
      case p@Aggregate(_, _, Join(_, _, _, _)) => true
      case p@Filter(_, _) => true
      case _ => false
    }
  }
}





