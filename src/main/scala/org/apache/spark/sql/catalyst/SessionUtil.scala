package org.apache.spark.sql.catalyst

import org.apache.spark.sql.SparkSession

/**
  * 2019-07-19 WilliamZhu(allwefantasy@gmail.com)
  */
object SessionUtil {
  def cloneSession(session: SparkSession) = {
    session.cloneSession()
  }
}
