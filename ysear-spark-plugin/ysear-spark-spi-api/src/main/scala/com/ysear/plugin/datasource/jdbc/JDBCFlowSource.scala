package com.ysear.plugin.datasource.jdbc

import com.ysear.plugin.datasource.dialect.OracleJdbcDialect
import com.ysear.spark.spi.source.{AbsFlowSource, FlowSourceParam}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects

import scala.collection.mutable

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class JDBCFlowSource(flowSourceParam: FlowSourceParam) extends AbsFlowSource{

  private val level: String = flowSourceParam.cacheLevel
  if(StringUtils.isNoneBlank(level)){
    storageLevel_=(level)
  }

  override def createDataFrame: DataFrame = {
    val source: JDBCBase = getDataSourceParam(flowSourceParam.sourceId).asInstanceOf[JDBCBase]
    val properties: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    properties.put("driver", source.getDriverClass)
    properties.put("url", source.getUrl)
    properties.put("user", source.getUserName)
    properties.put("password", source.getPassWord)
    val alias: String = flowSourceParam.alias
    // 这里应该设置表
    if (StringUtils.isNotBlank(alias)) {
      properties.put("dbtable", alias)
    } else {
      val querySql: String = flowSourceParam.querySql
      if (StringUtils.isNotBlank(querySql)) {
        properties.put("query", querySql)
      } else {
        throw new IllegalArgumentException("no dbtable or query attribute to set!")
      }
    }
    val df: DataFrame = super.sqlContext.read.format("jdbc").options(properties).load()
    if(StringUtils.isNoneBlank(alias)){
      df.createOrReplaceTempView(alias)
      logInfo(s"register spark table: ${alias}")
    }
    if(cached){
      cacheDataset(this,alias)
    }
    df
  }

  def getSourceAttribute: FlowSourceParam = flowSourceParam
}

object JDBCFlowSource {
  JdbcDialects.registerDialect(new OracleJdbcDialect())
}
