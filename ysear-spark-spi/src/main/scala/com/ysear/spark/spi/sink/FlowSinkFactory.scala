package com.ysear.spark.spi.sink

import org.apache.spark.internal.Logging
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/5
 **/
trait FlowSinkFactory extends Logging{
  /**
   *  工厂类型
   * @return
   */
  def getType:String


  /**
   *  生成sink节点对象
   * @param flowSinkParam sink参数
   * @return
   */
  def create(element: Element):AbsFlowSink

}
