package com.ysear.spark.core.transform

import com.ysear.spark.common.Constants
import com.ysear.spark.core.flow.SparkFlowReader
import org.dom4j.{Document, Element, Node}

import java.util
import scala.collection.mutable.ListBuffer

/**
 * @ClassName FlowTransformReader.scala
 * @createTime 2022年12月27日 17:05:00
 */
class FlowTransformReader(var _xml: Document) extends SparkFlowReader(_xml, false) with java.util.Enumeration[AbsTransform] {

  private val transforms: ListBuffer[AbsTransform] = readTransformNode()

  if (null == transforms || transforms.isEmpty) {
    throw new IllegalStateException("flow xml > transforms > transform")
  }

  private val iterator: Iterator[AbsTransform] = transforms.iterator


  def readTransformNode(): ListBuffer[AbsTransform] = {
    val transformNodes: ListBuffer[AbsTransform] = new ListBuffer[AbsTransform]
    val nodes: util.List[Node] = _xml.selectNodes(Constants.TRANSFORM_NODE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    // 遍历多有的dataSource
    nodes.forEach((elem: Node) => {
      val element: Element = elem.asInstanceOf[Element]
      val `type`: String = element.attributeValue("type")
      val source: AbsTransform = newTransformNode(`type`, element)
      transformNodes.append(source)
    })
    transformNodes
  }


  def newTransformNode(`type`: String,element: Element): AbsTransform = {
    val tableName: String = element.attributeValue("tableName")
    val repartition: Int = Integer.parseInt(Option.apply(element.attributeValue("repartition")).getOrElse(10).toString)
    val sql: String = element.getData.asInstanceOf[String]
    new SqlFlowTransform(`type`,sql,tableName,repartition)
  }


  override def hasMoreElements: Boolean = iterator.hasNext

  override def nextElement(): AbsTransform = iterator.next()
}
