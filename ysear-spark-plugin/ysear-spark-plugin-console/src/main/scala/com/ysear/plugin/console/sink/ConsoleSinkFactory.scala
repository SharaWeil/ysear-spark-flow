package com.ysear.plugin.console.sink

import com.ysear.plugin.datasource.BaseFlowSinkFactory
import com.ysear.spark.common.YsearDataType
import com.ysear.spark.spi.sink.{AbsFlowSink, FlowSinkParam}
import org.dom4j.Element


/*
 * @Author Administrator
 * @Date 2023/1/7
 **/

class ConsoleSinkFactory extends BaseFlowSinkFactory {
  /**
   * 工厂类型
   *
   * @return
   */
  override def getType: String = YsearDataType.CONSOLE.name()

  override def create(element: Element): AbsFlowSink = {
    new ConsoleSink(createParam(element))
  }

  override def createParam(element: Element): FlowSinkParam = {
    val `type`: String = element.attributeValue("type")
    val rowNum: Int = Integer.parseInt(Option.apply(element.attributeValue("rowNum")).getOrElse(20).toString)
    logInfo(s"found one sink:[type->${`type`}]")
    val tableName: String = element.attributeValue("tableName")
    new ConsoleSinkParam(rowNum, tableName)
  }
}