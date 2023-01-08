package com.ysear.spark.core

import com.ysear.spark.common.Constants
import com.ysear.spark.core.flow.SparkFlowReader
import com.ysear.spark.spi.PluginManager
import com.ysear.spark.spi.datasource.{BaseDataSource, FlowDataSourceFactory}
import org.dom4j.{Document, Element, Node}

import java.util
import scala.collection.mutable.ListBuffer


/**
 * @ClassName FlowDataSourceReader.scala
 * @createTime 2022年12月27日 09:53:00
 */
class FlowDataSourceReader(var _xml:Document,_pluginManager:PluginManager) extends SparkFlowReader(_xml,false) with java.util.Enumeration[BaseDataSource]{

  def this(_xml:Document){
    this(_xml,null)
  }

  private[this] val sources: ListBuffer[BaseDataSource] = readSource()

  if (null == sources || sources.isEmpty) {
    throw new IllegalStateException("flow xml > dataSources > dataSource")
  }

  private val iterator: Iterator[BaseDataSource] = sources.iterator

  private def readSource(): ListBuffer[BaseDataSource] ={
    val sources:ListBuffer[BaseDataSource] = new ListBuffer[BaseDataSource]
    val dataSources: util.List[Node] = xml.selectNodes(Constants.DATA_SOURCE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    // 遍历多有的dataSource
    dataSources.forEach((elem:Node)=>{
      val element: Element = elem.asInstanceOf[Element]
      val `type`: String = element.attributeValue("type")
      val id: String = element.attributeValue("id")
      val sourceFactory: FlowDataSourceFactory = _pluginManager.getDataSource(`type`)
      val source:BaseDataSource = sourceFactory.create(element)
      sources.append(source)
    })
    sources
  }


  override def hasMoreElements: Boolean = iterator.hasNext

  override def nextElement(): BaseDataSource = iterator.next()
}
