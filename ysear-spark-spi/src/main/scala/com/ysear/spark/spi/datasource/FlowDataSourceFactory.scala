package com.ysear.spark.spi.datasource

import com.ysear.spark.spi.BaseParam
import org.apache.spark.internal.Logging
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
trait FlowDataSourceFactory extends Logging{
  /**
   *  工厂类型
   * @return
   */
  def getType:String

  def create(element: Element):BaseDataSource
}
