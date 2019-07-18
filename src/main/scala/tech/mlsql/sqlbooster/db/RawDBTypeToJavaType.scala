package tech.mlsql.sqlbooster.db

import com.alibaba.druid.util.JdbcConstants
import com.mysql.cj.MysqlType

/**
  * 2019-07-18 WilliamZhu(allwefantasy@gmail.com)
  */
object RawDBTypeToJavaType {
  def convert(dbType: String, name: String) = {
    dbType match {
      case JdbcConstants.MYSQL => MysqlType.valueOf(name.toUpperCase()).getJdbcType
      case _ => throw new RuntimeException(s"dbType ${dbType} is not supported yet")
    }

  }

}
