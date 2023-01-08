package com.ysear.spark.core.flow

import com.ysear.spark.common.Constants
import com.ysear.spark.spi.PluginManager
import com.ysear.spark.spi.source.{AbsFlowSource, FlowSourceFactory,FlowSourceParam}
import org.dom4j.{Document, Element, Node}

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @ClassName SparkSourceNodeReader.scala
 * @createTime 2022年12月27日 16:15:00
 */
class FlowSourceReader(var _xml:Document,_pluginManager:PluginManager) extends SparkFlowReader(_xml,false) with java.util.Enumeration[AbsFlowSource] {

  private val sources: ListBuffer[AbsFlowSource] = readSourceNode()

  if (null == sources || sources.isEmpty) {
    throw new IllegalStateException("flow xml > dataSources > dataSource")
  }

  private val iterator: Iterator[AbsFlowSource] = sources.iterator

  def this(_xml:Document){
    this(_xml,null)
  }

  def readSourceNode(): ListBuffer[AbsFlowSource] = {
    val sourceNodes:ListBuffer[AbsFlowSource] = new ListBuffer[AbsFlowSource]
    val nodes: util.List[Node] = xml.selectNodes(Constants.SOURCE_NODE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    // 遍历多有的dataSource
    nodes.forEach((elem:Node)=>{
      val element: Element = elem.asInstanceOf[Element]
      val `type`: String = element.attributeValue("type")
      val factory: FlowSourceFactory = _pluginManager.getFlowSource(`type`)
      val source: AbsFlowSource = factory.create(element)
      sourceNodes.append(source)
      val attributes: FlowSourceParam = source.getSourceAttribute
      logInfo(s"found one sourceNode:[type->${`type`},sourceId->${attributes.sourceId},alias->${attributes.alias}]")
    })
    sourceNodes
  }


  override def hasMoreElements: Boolean = iterator.hasNext

  override def nextElement(): AbsFlowSource = iterator.next()
}
