package org.apache.spark.sql.catalyst.optimizer.rewrite.matcher

import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.optimizer.RewriteHelper
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer


case class CompensationExpressions(isRewriteSuccess: Boolean, compensation: Seq[Expression])

case class RangeCondition(key: Expression, lowerBound: Option[Literal], upperBound: Option[Literal],
                          includeLowerBound: Boolean,
                          includeUpperBound: Boolean) {

  def toExpression: Seq[Expression] = {
    (lowerBound, upperBound) match {
      case (None, None) => Seq()
      case (Some(l), None) => if (includeLowerBound)
        Seq(GreaterThanOrEqual(key, l)) else Seq(GreaterThan(key, l))
      case (None, Some(l)) => if (includeUpperBound)
        Seq(LessThanOrEqual(key, l)) else Seq(LessThan(key, l))
      case (Some(a), Some(b)) =>
        val aSeq = if (includeLowerBound)
          Seq(GreaterThanOrEqual(key, a)) else Seq(GreaterThan(key, a))
        val bSeq = if (includeUpperBound)
          Seq(LessThanOrEqual(key, b)) else Seq(LessThan(key, b))
        aSeq ++ bSeq
    }
  }

  def isSubRange(other: RangeCondition) = {
    this.key.semanticEquals(other.key) &&
      greaterThenOrEqual(this.lowerBound, other.lowerBound) &&
      greaterThenOrEqual(other.upperBound, this.upperBound)
  }

  def greaterThenOrEqual(lit1: Option[Literal], lit2: Option[Literal]) = {
    (lit1, lit2) match {
      case (None, None) => true
      case (Some(l), None) => true
      case (None, Some(l)) => true
      case (Some(a), Some(b)) =>
        a.dataType match {

          case ShortType | IntegerType | LongType | FloatType | DoubleType => a.value.toString.toDouble >= b.value.toString.toDouble
          case StringType => a.value.toString >= b.value.toString
          case _ => throw new RuntimeException("not support type")
        }
    }
  }

  def +(other: RangeCondition) = {
    assert(this.key.semanticEquals(other.key))


    val _lowerBound = if (greaterThenOrEqual(this.lowerBound, other.lowerBound))
      (this.lowerBound, this.includeLowerBound) else (other.lowerBound, other.includeLowerBound)

    val _upperBound = if (greaterThenOrEqual(this.upperBound, other.upperBound))
      (other.upperBound, other.includeUpperBound) else (this.upperBound, this.includeUpperBound)
    RangeCondition(key, _lowerBound._1, _upperBound._1, _lowerBound._2, _upperBound._2)
  }


}


trait ExpressionMatcher extends ExpressionMatcherHelper {
  val DEFAULT = CompensationExpressions(false, Seq())

  def compare(query: Seq[Expression], view: Seq[Expression]): CompensationExpressions
}

trait ExpressionMatcherHelper extends RewriteHelper {
  def isSubSetOf(e1: Seq[Expression], e2: Seq[Expression]) = {
    val zipCount = Math.min(e1.size, e2.size)
    (0 until zipCount).map { index => if (e1(index).semanticEquals(e2(index))) 0 else 1 }.sum == 0
  }

  def subset[T](e1: Seq[T], e2: Seq[T]) = {
    assert(e1.size >= e2.size)
    if (e1.size == 0) Seq[Expression]()
    e1.slice(e2.size, e1.size)
  }
}

/**
  * Here we compare where conditions
  *
  *  1. Equal
  *  2. NoEqual (greater/less)
  *  3. others
  */
class WhereMatcher extends ExpressionMatcher {

  override def compare(queryConjunctivePredicates: Seq[Expression],
                       viewConjunctivePredicates: Seq[Expression]
                      ): CompensationExpressions = {

    val compensationCond = ArrayBuffer[Expression]()

    if (viewConjunctivePredicates.size > queryConjunctivePredicates.size) return DEFAULT

    // equal expression compare
    val viewEqual = extractEqualConditions(viewConjunctivePredicates)
    val queryEqual = extractEqualConditions(queryConjunctivePredicates)

    // if viewEqual are not subset of queryEqual, then it will not match.
    if (!isSubSetOf(viewEqual, queryEqual)) return DEFAULT
    compensationCond ++= subset[Expression](queryEqual, viewEqual)

    // less/greater expressions compare

    // make sure all less/greater expression with the same presentation
    // for example if exits a < 3 && a>=1 then we should change to RangeCondition(a,1,3)
    // or b < 3 then RangeCondition(b,None,3)
    val viewRange = extractRangeConditions(viewConjunctivePredicates).map(convertRangeCon)
    val queryRange = extractRangeConditions(queryConjunctivePredicates).map(convertRangeCon)

    // combine something like
    // RangeCondition(a,1,None),RangeCondition(a,None,3) into RangeCondition(a,1,3)

    def combineAndMergeRangeCondition(items: Seq[RangeCondition]) = {
      items.groupBy(f => f.key).map { f =>
        val first = f._2.head.copy(lowerBound = None, upperBound = None)
        f._2.foldLeft(first) { (result, item) =>
          result + item
        }
      }
    }

    val viewRangeCondition = combineAndMergeRangeCondition(viewRange).toSeq
    val queryRangeCondtion = combineAndMergeRangeCondition(queryRange).toSeq

    //again make sure viewRangeCondition.size is small queryRangeCondtion.size
    if (viewRangeCondition.size > queryRangeCondtion.size) return DEFAULT

    //all view rangeCondition  should a  SubRangeCondition of query
    val isRangeMatch = viewRangeCondition.map { viewRC =>
      queryRangeCondtion.map(queryRC => if (viewRC.isSubRange(queryRC)) 1 else 0).sum
    }.sum == viewRangeCondition.size

    if (!isRangeMatch) return DEFAULT

    compensationCond ++= (subset[RangeCondition](queryRangeCondtion, viewRangeCondition).flatMap(_.toExpression))

    // other conditions compare
    val viewResidual = extractResidualConditions(viewConjunctivePredicates)
    val queryResidual = extractResidualConditions(queryConjunctivePredicates)
    if (!isSubSetOf(viewResidual, queryResidual)) return DEFAULT
    compensationCond ++= subset[Expression](queryResidual, viewResidual)

    // return the compensation expressions
    CompensationExpressions(true, compensationCond)
  }


  val equalCon = (f: Expression) => {
    f.isInstanceOf[EqualNullSafe] || f.isInstanceOf[EqualTo]
  }

  val convertRangeCon = (f: Expression) => {
    f match {
      case GreaterThan(a, v@Literal(_, _)) => RangeCondition(a, Option(v), None, false, false)
      case GreaterThan(v@Literal(_, _), a) => RangeCondition(a, None, Option(v), false, false)
      case GreaterThanOrEqual(a, v@Literal(_, _)) => RangeCondition(a, Option(v), None, true, false)
      case GreaterThanOrEqual(v@Literal(_, _), a) => RangeCondition(a, None, Option(v), false, true)
      case LessThan(a, v@Literal(_, _)) => RangeCondition(a, None, Option(v), false, false)
      case LessThan(v@Literal(_, _), a) => RangeCondition(a, Option(v), None, false, true)
      case LessThanOrEqual(a, v@Literal(_, _)) => RangeCondition(a, None, Option(v), false, true)
      case LessThanOrEqual(v@Literal(_, _), a) => RangeCondition(a, Option(v), None, true, false)
    }
  }

  val rangeCon = (f: Expression) => {
    f match {
      case GreaterThan(_, Literal(_, _)) | GreaterThan(Literal(_, _), _) => true
      case GreaterThanOrEqual(_, Literal(_, _)) | GreaterThanOrEqual(Literal(_, _), _) => true
      case LessThan(_, Literal(_, _)) | LessThan(Literal(_, _), _) => true
      case LessThanOrEqual(_, Literal(_, _)) | LessThanOrEqual(Literal(_, _), _) => true
      case _ => false
    }
  }


  def extractEqualConditions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filter(equalCon)
  }

  def extractRangeConditions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filter(rangeCon)
  }

  def extractResidualConditions(conjunctivePredicates: Seq[Expression]) = {
    conjunctivePredicates.filterNot(equalCon).filterNot(rangeCon)
  }
}
