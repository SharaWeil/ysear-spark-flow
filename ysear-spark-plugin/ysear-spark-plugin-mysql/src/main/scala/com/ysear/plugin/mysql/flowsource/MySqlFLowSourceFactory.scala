package com.ysear.plugin.mysql.flowsource

import com.ysear.plugin.datasource.BaseFlowSourceFactory
import com.ysear.spark.common.YsearDataType
import com.ysear.spark.spi.source.{AbsFlowSource, FlowSourceFactory, FlowSourceParam}
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class MySqlFLowSourceFactory extends BaseFlowSourceFactory{
  /**
   * 工厂类型
   *
   * @return
   */
  override def getType: String = YsearDataType.MYSQL.name()

  override def create(element: Element): AbsFlowSource = {
    new MySqlFlowSource(createParam(element))
  }

  override def createParam(element: Element): FlowSourceParam = {
    super.createParam(element)
  }
}
