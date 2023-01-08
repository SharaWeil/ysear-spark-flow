package com.ysear.spark.core.flow

import com.ysear.spark.common.Constants
import com.ysear.spark.core.udf.UDF
import org.apache.spark.internal.Logging
import org.dom4j.{Document, Element, Node}

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class SparkFlowReader(var xml:Document) extends Logging with Serializable {
  var _ignoreCase:Boolean = _

  protected var globalParameter:mutable.HashMap[String,String] = _

  def this(_xml:Document,ignoreCase:Boolean){
    this(_xml)
    this._ignoreCase = ignoreCase
  }

  def readFlowInfo(): FlowInfo = {
    val infos:util.List[Node] = xml.selectNodes(Constants.FLOW_INFO_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    val info: Element = infos.get(0).asInstanceOf[Element]
    val name: String = info.element("name").getText
    val file: String = info.element("file").getText
    val version: String = info.element("version").getText
    logInfo(s"flow info:【name=$name,file=$file,version=$version】")
    FlowInfo(name,file,version)
  }


  def getUdfs: ListBuffer[UDF] = {
    val udfs = new ListBuffer[UDF]
    val udfNodes:util.List[Node] = xml.selectNodes(Constants.UDF_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    udfNodes.forEach((x:Node)=>{
      val udf: Element = x.asInstanceOf[Element]
      val name: String = udf.attributeValue("name")
      val classFull: String = udf.attributeValue("class")
      val returnType: String = udf.attributeValue("returnType")
      logInfo(s"Found UDF: 【name=$name,classFullName:$classFull,returnType:$returnType】")
      udfs.append(new UDF(name, classFull, returnType))
    })
    udfs
  }


  def getSparkConf:mutable.HashMap[String,String] = {
    val sparkConfig: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    val config: util.List[Node] = xml.selectNodes(Constants.CONFIG_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
    if(null !=config && !config.isEmpty){
      config.forEach((x:Node)=>{
        val node: Element = x.asInstanceOf[Element]
        val elements: util.List[Element] = node.elements().asInstanceOf[util.List[Element]]
        if(null != elements && !elements.isEmpty){
          elements.forEach(x=>{
            val key: String = x.getName
            val value: String = x.getText
            sparkConfig.put(key,value)
          })
        }
      })
    }
    sparkConfig
  }
}
