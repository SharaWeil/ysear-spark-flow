package com.ysear.spark.spi.sink

import com.ysear.spark.spi.{BaseParam, DataSourceManager, Persistable}
import com.ysear.spark.spi.datasource.BaseDataSource
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import java.util.Optional

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
abstract class AbsFlowSink(val viewName:String){
  @transient private var _sqlContext: SQLContext = _

  @transient private var _conf: Configuration = _

  private[this] var _dataSourceManager:DataSourceManager[BaseDataSource] = _

  private var _sql: String = _

  def sql_=(value: String): Unit = {
    _sql = value
  }

  def pluginManager: DataSourceManager[BaseDataSource] = _dataSourceManager

  def pluginManager_=(value: DataSourceManager[BaseDataSource]): Unit = {
    _dataSourceManager = value
  }

  def sqlContext: SQLContext = _sqlContext

  def sqlContext_=(value: SQLContext): Unit = {
    _sqlContext = value
  }
  def conf: Configuration = _conf

  def conf_=(value: Configuration): Unit = {
    _conf = value
  }

  /**
   *
   * @param sourceId
   * @return
   */
  def getDataSourceParam(sourceId:String):BaseParam= {
    _dataSourceManager.getDataSource(sourceId)
  }
  def isStream: Boolean = false

  def sink(dataFrame: DataFrame)

}
