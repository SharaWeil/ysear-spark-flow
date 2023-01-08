package com.ysear.spark.core.flow

import com.ysear.spark.common.Constants
import com.ysear.spark.spi.PluginManager
import com.ysear.spark.spi.sink.{AbsFlowSink, FlowSinkFactory, FlowSinkParam}
import org.dom4j.{Document, Element, Node}

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @ClassName FlowSinkReader.scala
 * @createTime 2022年12月27日 17:33:00
 */
class FlowSinkReader(var _xml: Document,_pluginManager:PluginManager) extends SparkFlowReader(_xml, false) with java.util.Enumeration[AbsFlowSink] {

  private val sinks: ListBuffer[AbsFlowSink] = readTransformNode()

  if (null == sinks || sinks.isEmpty) {
    throw new IllegalStateException("flow xml > targets > target")
  }

  private val iterator: Iterator[AbsFlowSink] = sinks.iterator

  def readTransformNode(): ListBuffer[AbsFlowSink] = {
    val sinkNodes: ListBuffer[AbsFlowSink] = new ListBuffer[AbsFlowSink]
    val nodes: util.List[Node] = xml.selectNodes(Constants.SINK_NODE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    // 遍历多有的dataSource
    nodes.forEach((elem: Node) => {
      val element: Element = elem.asInstanceOf[Element]
      val `type`: String = element.attributeValue("type")
      val flowSinkFactory: FlowSinkFactory = _pluginManager.getFlowSInk(`type`)
      val flowSink: AbsFlowSink = flowSinkFactory.create(element)
      sinkNodes.append(flowSink)
    })
    sinkNodes
  }


  override def hasMoreElements: Boolean = iterator.hasNext

  override def nextElement(): AbsFlowSink = iterator.next()
}
