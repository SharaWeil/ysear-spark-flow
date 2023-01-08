package com.ysear.spark.spi

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
trait DataSourceManager[T] extends Serializable{
  def registerDataSource(t:T):Unit

  def getDataSource(sourceId:String):T
}
