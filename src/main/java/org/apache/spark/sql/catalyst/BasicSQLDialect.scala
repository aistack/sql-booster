package org.apache.spark.sql.catalyst

import java.sql.Connection

import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * 2019-07-14 WilliamZhu(allwefantasy@gmail.com)
  */
class BasicSQLDialect extends SQLDialect {
  override def canHandle(url: String): Boolean = url.toLowerCase().startsWith("jdbc:mysql")

  override def quote(name: String): String = {
    "`" + name.replace("`", "``") + "`"
  }

  override def explainSQL(sql: String): String = s"EXPLAIN $sql"

  override def relation(relation: LogicalRelation): String = {
    relation.toString()
  }

  override def maybeQuote(name: String): String = {
    name
  }

  override def getIndexes(conn: Connection, url: String, tableName: String): Set[String] = {
    Set()
  }

  override def getTableStat(conn: Connection, url: String, tableName: String): (Option[BigInt], Option[Long]) = {
    (None, None)
  }
}
