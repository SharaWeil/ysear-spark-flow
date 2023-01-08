package com.ysear.spark.spi.source

import com.ysear.spark.spi.datasource.BaseDataSource
import com.ysear.spark.spi.{BaseParam, DataSourceManager, Persistable}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import java.util.Optional

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
abstract class AbsFlowSource extends Persistable{

  @transient private var _sqlContext: SQLContext = _

  @transient private var _conf: Configuration = _

  private[this] var _dataSourceManager:DataSourceManager[BaseDataSource] = _

  private var _viewName: String = _

  private var _sql: String = _

  def this(viewName: String,sql:String)={
    this()
    this._viewName = viewName
    this._sql = sql
  }

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
  def vieName: String = _viewName

  def vieName_=(value: String): Unit = {
    _viewName = value
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

  def createDataFrame: DataFrame

  def getSourceAttribute:FlowSourceParam


  /**
   *  缓存df
   * @param persistable
   * @param tableName
   */
  protected[AbsFlowSource] def cacheDataset(persistable: Persistable, tableName: String): Unit = {
    val storageLevel: String = Optional.ofNullable[String](persistable.storageLevel).orElse("MEMORY_ONLY")
    if ("MEMORY_ONLY".equalsIgnoreCase(storageLevel)) this.sqlContext.cacheTable(tableName)
    else if ("DISK_ONLY".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.DISK_ONLY)
    else if ("MEMORY_AND_DISK".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.MEMORY_AND_DISK)
    else if ("MEMORY_AND_DISK_SER".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.MEMORY_AND_DISK_SER)
    else throw new IllegalArgumentException("unsupported storageLevel Value " + storageLevel + " in viewName " + tableName)
  }

}
