package com.ysear.plugin.datasource

import com.ysear.spark.spi.sink.{AbsFlowSink, FlowSinkFactory, FlowSinkParam}
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/8
 **/
abstract class BaseFlowSinkFactory extends FlowSinkFactory{
  /**
   * 工厂类型
   *
   * @return
   */
  override def getType: String = ???

  /**
   * 生成sink节点对象
   *
   * @param flowSinkParam sink参数
   * @return
   */
  def create(element: Element):AbsFlowSink

  /**
   *  根据节点创建对应的参数
   * @param element
   * @return
   */
  def createParam(element: Element): FlowSinkParam
}
