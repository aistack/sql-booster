package org.apache.spark.sql.catalyst.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal}
import org.apache.spark.sql.catalyst.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable.ArrayBuffer

/**
  * 2019-07-15 WilliamZhu(allwefantasy@gmail.com)
  */
class AggMatcher(
                  query: Seq[Expression],
                  view: Seq[Expression],
                  isGroupByEqual: Boolean,
                  targetViewLogicalPlan: LogicalPlan) extends ExpressionMatcher {
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

    // let's take care the first situation, if there are count(*) in query, then
    // count(*) should also be in view and we should replace it with sum(count_view)
    val queryCountStar = getCountStartList(query)
    val viewCountStar = getCountStartList(view)

    if (queryCountStar.size > 0 && viewCountStar == 0) return DEFAULT

    var queryReplaceCountStar = query

    if (!isGroupByEqual && queryCountStar.size > 0) {
      val replaceItem = viewCountStar.head
      queryReplaceCountStar = query map { expr =>
        expr transformDown {
          case Alias(agg@AggregateExpression(Count(Seq(Literal(1, IntegerType))), _, _, _), name) =>
            Alias(agg.copy(aggregateFunction = Sum(replaceItem.child)), name)()
        }
      }
    }

    // let's take care the second situation, if there are AVG(k) in query,then count(*)
    // should also be in view and we should replace it with avg(sum(k)/view_count(*))

    val queryAvg = getAvgList(query)

    if (queryAvg.size > 0 && viewCountStar == 0) return DEFAULT

    var queryReplaceAvg = queryAvg

    //    if (!isGroupByEqual && queryAvg.size > 0) {
    //      val replaceItem = viewCountStar.head
    //      val replaceAttrributeReference = AttributeReference(replaceItem.name, LongType)()
    //      queryReplaceAvg = queryAvg map { expr =>
    //        expr transformDown {
    //          case a@Alias(agg@AggregateExpression(Average(ar@_), _, _, _), name) =>
    //            val sum = agg.copy(aggregateFunction = Sum(ar))
    //            Alias(Divide(sum, replaceAttrributeReference), name)()
    //        }
    //      }
    //    }

    val (queryLeft, viewLeft, common) = extractTheSameExpressionsWithPosition(view, query)
    if (viewLeft.size > 0) return DEFAULT
    if (common.size != view.size) return DEFAULT
    CompensationExpressions(true, queryLeft)

  }

  private def extractTheSameExpressionsWithPosition(view: Seq[Expression], query: Seq[Expression])

  = {
    val viewLeft = ArrayBuffer[Expression](view: _*)
    val queryLeft = ArrayBuffer[Expression](query: _*)
    val common = ArrayBuffer[Expression]()

    (0 until view.size).foreach { index =>
      if (view(index).semanticEquals(query(index))) {
        common += view(index)
        viewLeft -= view(index)
        queryLeft -= query(index)
      }
    }

    (viewLeft, queryLeft, common)
  }

  private def getCountStartList(items: Seq[Expression]) = {
    val queryCountStar = ArrayBuffer[Alias]()
    items.foreach { expr =>
      expr transformDown {
        case a@Alias(AggregateExpression(Count(Seq(Literal(1, IntegerType))), _, _, _), name) =>
          queryCountStar += a
          a
      }

    }
    queryCountStar
  }

  private def getAvgList(items: Seq[Expression]) = {
    val avgList = ArrayBuffer[Alias]()
    items.foreach { expr =>
      expr transformDown {
        case a@Alias(AggregateExpression(Average(ar@_), _, _, _), name) =>
          avgList += a
          a
      }
    }
    avgList
  }
}

case class CountRewrite


