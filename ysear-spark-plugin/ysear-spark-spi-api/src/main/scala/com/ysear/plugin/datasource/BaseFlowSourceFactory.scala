package com.ysear.plugin.datasource

import com.ysear.spark.spi.source.{AbsFlowSource, FlowSourceFactory, FlowSourceParam}
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/8
 *
 */
abstract class BaseFlowSourceFactory extends FlowSourceFactory{
  /**
   * 工厂类型
   *
   * @return
   */
  override def getType: String

  def create(element: Element): AbsFlowSource

  def createParam(elem: Element): FlowSourceParam = {
    val element: Element = elem.asInstanceOf[Element]
    val `type`: String = element.attributeValue("type")
    var sourceId: String = element.attributeValue("sourceId")
    var alias: String = element.attributeValue("alias")
    val cache: String = element.attributeValue("cache")
    val repartition: Int = Integer.parseInt(Option.apply(element.attributeValue("repartition")).getOrElse(10).toString)
    sourceId = Option.apply(sourceId)
      .getOrElse(throw new RuntimeException(s"Flow source must have a data source ID,source id: $sourceId"))
    alias = Option.apply(alias)
      .getOrElse(throw new RuntimeException(s"Flow source must have an alias,alias: $alias"))
    val sql: String = element.getData.asInstanceOf[String]
    val sourceParam = new FlowSourceParam(`type`, sourceId, alias, sql)
    sourceParam.cacheLevel_=(cache)
    sourceParam.partition_=(repartition)
    sourceParam
  }
}
