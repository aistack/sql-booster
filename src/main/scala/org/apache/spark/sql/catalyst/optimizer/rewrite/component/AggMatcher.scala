package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Divide, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule._
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class AggMatcher(rewriteContext: RewriteContext
                ) extends ExpressionMatcher {
  /**
    * when the view/query both has count(*), and the group by condition in query isSubset(not equal) of view,
    * then we should replace query count(*) with SUM(view_count(*)). for example:
    *
    * view: select count(*) as a from table1 group by m;
    * query: view1 => select count(*) as a from table1 group by m,c
    *
    * target: select sum(a) from view1 group by c
    *
    * Another situation we should take care  is AVG:
    *
    * view: select count(*) as a from table1 group by m;
    * query: view1 => select avg(k) from table1 group by m,c
    *
    * target: select sum(k)/a from view1 group by c
    *
    *
    */
  override def compare: CompensationExpressions = {

    val query = rewriteContext.processedComponent.get().queryAggregateExpressions
    val view = rewriteContext.processedComponent.get().viewAggregateExpressions

    // let's take care the first situation, if there are count(*) in query, then
    // count(*) should also be in view and we should replace it with sum(count_view)
    val queryCountStar = getCountStartList(query)
    val viewCountStar = getCountStartList(view)

    if (queryCountStar.size > 0 && viewCountStar == 0) return RewriteFail.AGG_NUMBER_UNMATCH(this)

    val viewProjectOrAggList = rewriteContext.viewLogicalPlan.get().tableLogicalPlan.output


    /**
      * let's take care the third situation, any agg filed both in view/query, we should replace it with new field in view
      *
      * query:
      *
      * SELECT deptno, COUNT(*) AS c, SUM(salary) AS s
      * FROM emps
      * GROUP BY deptno
      *
      * view:
      *
      * SELECT empid, deptno, COUNT(*) AS c, SUM(salary) AS s
      * FROM emps
      * GROUP BY empid, deptno
      *
      * target:
      *
      * SELECT deptno, SUM(c), SUM(s)
      * FROM mv
      * GROUP BY deptno
      *
      * here we should convert  SUM(salary) to SUM(s) or s
      */


    val exactlySame = query.filterNot { item =>
      item match {
        case a@Alias(agg@AggregateExpression(Average(ar@_), _, _, _), name) => false
        case a@Alias(agg@AggregateExpression(Count(_), _, _, _), name) => false
        case _ => true
      }
    }


    val success = exactlySame.map { item =>
      if (view.filter { f =>
        cleanAlias(f).semanticEquals(cleanAlias(item))
      }.size > 0) 1 else 0
    }.sum == exactlySame.size

    if (!success) return RewriteFail.AGG_COLUMNS_UNMATCH(this)

    var queryReplaceAgg = query

    queryReplaceAgg = queryReplaceAgg.map { item =>
      item transformUp  {
        case a@Alias(agg@AggregateExpression(Average(ar@_), _, _, _), name) => a
        case a@Alias(agg@AggregateExpression(Count(_), _, _, _), name) => a
        case a@Alias(agg@AggregateExpression(_, _, _, _), name) =>
          val (vItem, index) = view.zipWithIndex.filter { case (vItem, index) =>
            cleanAlias(vItem).semanticEquals(cleanAlias(a))
          }.head
          val newVItem = vItem transformDown {
            case a@AttributeReference(_, _, _, _) => viewProjectOrAggList(index)
          }
          Alias(cleanAlias(newVItem), name)()
      }
    }


    var queryReplaceCountStar = queryReplaceAgg

    if (queryCountStar.size > 0) {
      val replaceItem = viewCountStar.head
      val arInViewTable = extractAttributeReferenceFromFirstLevel(viewProjectOrAggList).filter { ar =>
        ar.name == replaceItem.asInstanceOf[Alias].name
      }.head

      queryReplaceCountStar = queryReplaceCountStar map { expr =>
        expr transformDown {
          case Alias(agg@AggregateExpression(Count(Seq(Literal(1, IntegerType))), _, _, _), name) =>
            Alias(agg.copy(aggregateFunction = Sum(arInViewTable)), name)()
        }
      }
    }

    // let's take care the second situation, if there are AVG(k) in query,then count(*)
    // should also be in view and we should replace it with sum(k)/view_count(*)

    val queryAvg = getAvgList(query)

    if (queryAvg.size > 0 && viewCountStar == 0) return RewriteFail.AGG_VIEW_MISSING_COUNTING_STAR(this)

    var queryReplaceAvg = queryReplaceCountStar

    if (queryAvg.size > 0) {
      val replaceItem = viewCountStar.head
      val arInViewTable = extractAttributeReferenceFromFirstLevel(viewProjectOrAggList).filter { ar =>
        ar.name == replaceItem.asInstanceOf[Alias].name
      }.head

      queryReplaceAvg = queryReplaceAvg.map { expr =>
        val newExpr = expr transformDown {
          case a@Alias(agg@AggregateExpression(Average(ar@_), _, _, _), name) =>
            // and ar should be also in viewProjectOrAggList
            val sum = agg.copy(aggregateFunction = Sum(ar))
            Alias(Divide(sum, arInViewTable), name)()
        }
        newExpr
      }
    }


    CompensationExpressions(true, queryReplaceAvg)

  }


  private def getCountStartList(items: Seq[Expression]) = {
    val queryCountStar = ArrayBuffer[Expression]()
    items.zipWithIndex.foreach { case (expr, index) =>
      expr transformDown {
        case a@Alias(AggregateExpression(Count(Seq(Literal(1, IntegerType))), _, _, _), name) =>
          queryCountStar += a
          a
      }

    }
    queryCountStar
  }

  private def getAvgList(items: Seq[Expression]) = {
    val avgList = ArrayBuffer[Expression]()
    items.foreach { expr =>
      expr transformDown {
        case a@Alias(AggregateExpression(Average(ar@_), _, _, _), name) =>
          avgList += a
          a
      }
    }
    avgList
  }

  private def cleanAlias(expr: Expression) = {
    expr match {
      case Alias(child, _) => child
      case _ => expr
    }
  }
}





