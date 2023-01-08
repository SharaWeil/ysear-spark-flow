package com.ysear.spark.core.transform

import com.ysear.spark.spi.Persistable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

import java.util.Optional
import scala.collection.mutable


/**
 * @ClassName AbsTransform.scala
 * @createTime 2022年12月27日 17:08:00
 */
abstract class AbsTransform extends Persistable{

  @transient private var _sqlContext: SQLContext = _

  @transient private var _conf: Configuration = _

  private var _viewName: String = _
  private var _partition: Int = 1


  def this(viewName: String,partition:Int) = {
    this()
    this._viewName = viewName
    this._partition = partition
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

  def isStream: Boolean = false

  def transform: DataFrame


  def getFullSql:String


  /**
   *  缓存df
   * @param persistable
   * @param tableName
   */
  protected[AbsTransform] def cacheDataset(persistable: Persistable, tableName: String): Unit = {
    val storageLevel: String = Optional.ofNullable[String](persistable.storageLevel).orElse("MEMORY_ONLY")
    if ("MEMORY_ONLY".equalsIgnoreCase(storageLevel)) this.sqlContext.cacheTable(tableName)
    else if ("DISK_ONLY".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.DISK_ONLY)
    else if ("MEMORY_AND_DISK".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.MEMORY_AND_DISK)
    else if ("MEMORY_AND_DISK_SER".equalsIgnoreCase(storageLevel)) this.sqlContext.sparkSession.catalog.cacheTable(tableName, StorageLevel.MEMORY_AND_DISK_SER)
    else throw new IllegalArgumentException("unsupported storageLevel Value " + storageLevel + " in viewName " + tableName)
  }
}
