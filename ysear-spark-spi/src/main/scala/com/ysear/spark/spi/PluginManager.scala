package com.ysear.spark.spi

import com.ysear.spark.spi.datasource.FlowDataSourceFactory
import com.ysear.spark.spi.sink.FlowSinkFactory
import com.ysear.spark.spi.source.FlowSourceFactory
import org.apache.spark.internal.Logging

import java.lang.String.format
import java.util
import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap

/*
 * @Author Administrator
 * @Date 2023/1/5
 * 插件管理类，管理通过spi加载的
 * DataSourceFactory
 * FlowSourceFactory,
 * FlowSinkFactory
 */
class PluginManager(spiLoader: YsearSpiLoader) extends Logging{
  private val flowSourcePlugins:ConcurrentHashMap[String,FlowSourceFactory] = new ConcurrentHashMap[String,FlowSourceFactory]()

  private val flowSinkFactory:ConcurrentHashMap[String,FlowSinkFactory] = new ConcurrentHashMap[String,FlowSinkFactory]()


  private val dataSourcePlugin:ConcurrentHashMap[String,FlowDataSourceFactory] = new ConcurrentHashMap[String,FlowDataSourceFactory]()


  val names:util.Set[String] = new util.HashSet[String]()


  // 加载flowSource工厂
  spiLoader.load(classOf[FlowSourceFactory]).foreach((factory: FlowSourceFactory)=>{
    val sourceType: String = factory.getType.toUpperCase()
    log.info(s"Registering datasource plugin: $sourceType")
    loadFlowSource(factory)
    log.info(s"Registered datasource plugin: $sourceType")
  })


  // 加载dataSource工厂
  spiLoader.load(classOf[FlowDataSourceFactory]).foreach((factory: FlowDataSourceFactory)=>{
    val sourceType: String = factory.getType.toUpperCase()
    log.info(s"Registering datasource plugin: $sourceType")
    if (!names.add(sourceType)) throw new IllegalStateException(format("Duplicate datasource plugins named '%s'", sourceType))
    loadDataSource(factory)
    log.info(s"Registered datasource plugin: $sourceType")
  })


  // 加载sink工厂
  spiLoader.load(classOf[FlowSinkFactory]).foreach((factory: FlowSinkFactory)=>{
    val sourceType: String = factory.getType.toUpperCase()
    log.info(s"Registering datasource plugin: $sourceType")
    loadFlowSink(factory)
    log.info(s"Registered datasource plugin: $sourceType")
  })


  def loadDataSource(factory: FlowDataSourceFactory): FlowDataSourceFactory = {
    val factoryType: String = factory.getType.toUpperCase
    dataSourcePlugin.put(factoryType,factory);
  }

  def loadFlowSource(factory: FlowSourceFactory): Unit = {
    val factoryType: String = factory.getType.toUpperCase
    flowSourcePlugins.put(factoryType,factory)
  }

  def loadFlowSink(factory: FlowSinkFactory): FlowSinkFactory = {
    val factoryType: String = factory.getType.toUpperCase
    flowSinkFactory.put(factoryType,factory)
  }

  def getFlowSource(sourceType:String): FlowSourceFactory ={
    val factory: FlowSourceFactory = flowSourcePlugins.get(sourceType.toUpperCase)
    if (null == factory){
      throw new RuntimeException(s"flowSource not found,sourceType: ${sourceType}")
    }
    factory
  }


  def getDataSource(sourceType:String): FlowDataSourceFactory ={
    val factory: FlowDataSourceFactory = dataSourcePlugin.get(sourceType.toUpperCase)
    if (null == factory){
      throw new RuntimeException(s"dataSource not found,sourceType: ${sourceType}")
    }
    factory
  }

  def getFlowSInk(sourceType:String): FlowSinkFactory ={
    val factory: FlowSinkFactory = flowSinkFactory.get(sourceType.toUpperCase)
    if (null == factory){
      throw new RuntimeException(s"flowSink not found,sourceType: ${sourceType}")
    }
    factory
  }
}
