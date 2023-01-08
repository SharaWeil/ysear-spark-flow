package com.ysear.spark.core

import com.ysear.spark.spi.DataSourceManager
import com.ysear.spark.spi.datasource.BaseDataSource

import java.util.concurrent.ConcurrentHashMap

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class DataSourceManagerImpl extends DataSourceManager[BaseDataSource] {
  private val dataSourceCache:ConcurrentHashMap[String,BaseDataSource] = new ConcurrentHashMap[String,BaseDataSource](32)

  override def registerDataSource(dataSource: BaseDataSource): Unit = {
    val sourceId: String = dataSource.id
    if (dataSourceCache.contains(sourceId)) {
      throw new RuntimeException("Duplicate data source ID")
    }
    dataSourceCache.put(sourceId,dataSource)
  }

  override def getDataSource(sourceId: String): BaseDataSource = {
    Option.apply(dataSourceCache.get(sourceId)) match {
      case Some(dataSource) => dataSource
      case None => throw new RuntimeException(s"dataSource not found,sourceId: ${sourceId}")
    }
  }
}
