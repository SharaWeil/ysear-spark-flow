package com.ysear.spark.spi.source

import com.ysear.spark.spi.sink.AbsFlowSink
import org.dom4j.Element

import javax.swing.text.FlowView.FlowStrategy

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
trait FlowSourceFactory {
  /**
   *  工厂类型
   * @return
   */
  def getType:String

  def create(element: Element):AbsFlowSource
}
