package com.ysear.spark.core.transform

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame

/**
 * @ClassName SqlFlowTransform.scala
 * @createTime 2022年12月27日 17:11:00
 */
class SqlFlowTransform(`type`:String,sql:String,tableName:String,partition:Int) extends AbsTransform(tableName,partition) {

  override def transform: DataFrame = {
    val df: DataFrame = sqlContext.sql(getFullSql)
    if (partition > 1) {
      df.repartition(partition)
    }
    if (StringUtils.isNotBlank(vieName)){
      df.createOrReplaceTempView(vieName)
      logInfo(s"register spark table: ${vieName}")
    }
    if (cached) {
      cacheDataset(this,vieName)
    }
    df
  }

  override def getFullSql: String = sql
}
