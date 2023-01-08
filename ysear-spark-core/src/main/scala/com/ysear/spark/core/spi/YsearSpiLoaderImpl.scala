package com.ysear.spark.core.spi

import com.ysear.spark.spi.YsearSpiLoader
import com.ysear.spark.spi.source.FlowSourceFactory

import java.io.InputStream
import java.net.URL
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer
import java.util.{List, Map, Properties}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class YsearSpiLoaderImpl extends YsearSpiLoader {

  val FACTORIES_RESOURCE_LOCATION = "META-INF/ysear.factories"
  val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
  val cache: ConcurrentHashMap[ClassLoader, mutable.HashMap[String, mutable.ListBuffer[String]]] = new ConcurrentHashMap[ClassLoader, mutable.HashMap[String, mutable.ListBuffer[String]]]

  var result: mutable.HashMap[String, mutable.ListBuffer[String]] = cache.get(classLoader)

  if (null == result) {
    result = new mutable.HashMap[String, mutable.ListBuffer[String]]()
  }

  val urls: util.Enumeration[URL] = classLoader.getResources(FACTORIES_RESOURCE_LOCATION)

  while (urls.hasMoreElements) {
    val properties: Properties = new Properties()
    val url: URL = urls.nextElement()
    val in: InputStream = url.openConnection().getInputStream
    properties.load(in)
    properties.entrySet().forEach(entity => {
      val factoryTypeName: String = entity.getKey.asInstanceOf[String].trim
      val factoryImplementationName: String = entity.getValue.asInstanceOf[String]
      result.getOrElseUpdate(factoryTypeName,new ListBuffer[String]).append(factoryImplementationName)
    })
  }
  cache.put(classLoader, result)

  override def load[S](service: Class[S]): ListBuffer[S] = {
    val listBuffer: ListBuffer[String] = result.get(service.getName) match {
      case Some(value) => value
      case None => new ListBuffer[String]
    }
    listBuffer.map(elem => {
      val clazz: Class[S] = classLoader.loadClass(elem).asInstanceOf[Class[S]]
      clazz.newInstance()
    })
  }
}

object YsearSpiLoaderImpl {
  def main(args: Array[String]): Unit = {
    val impl: YsearSpiLoaderImpl = new YsearSpiLoaderImpl
    impl.load(classOf[FlowSourceFactory])
  }
}
