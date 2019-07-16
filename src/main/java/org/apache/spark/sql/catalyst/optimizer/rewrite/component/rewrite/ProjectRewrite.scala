package org.apache.spark.sql.catalyst.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{LogicalPlanRewrite, RewritedLogicalPlan, ViewLogicalPlan}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class ProjectRewrite(viewLogicalPlan: ViewLogicalPlan) extends LogicalPlanRewrite {

  override def rewrite(plan: LogicalPlan): LogicalPlan = {

    val projectOrAggList = viewLogicalPlan.tableLogicalPlan.output

    def rewriteProject(plan: LogicalPlan): LogicalPlan = {
      plan match {
        case Project(projectList, child) =>
          val newProjectList = projectList.map { expr =>
            expr transformDown {
              case a@AttributeReference(name, dt, _, _) =>
                extractAttributeReferenceFromFirstLevel(projectOrAggList).filter(f => attributeReferenceEqual(a, f)).head
            }
          }.map(_.asInstanceOf[NamedExpression])
          Project(newProjectList, child)
        case RewritedLogicalPlan(inner, _) => rewriteProject(inner)
        case _ => plan
      }
    }

    val newPlan = rewriteProject(plan)
    _back(RewritedLogicalPlan(newPlan, false))
  }
}
