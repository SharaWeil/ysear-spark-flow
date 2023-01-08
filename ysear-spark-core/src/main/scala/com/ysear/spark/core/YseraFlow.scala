package com.ysear.spark.core

import com.google.common.collect.ImmutableMap
import com.ysear.spark.core.flow.{FlowInfo, FlowSinkReader, FlowSourceReader, SparkFlowReader}
import com.ysear.spark.core.listener.FlowSparkListener
import com.ysear.spark.core.spi.YsearSpiLoaderImpl
import com.ysear.spark.core.transform.{AbsTransform, FlowTransformReader}
import com.ysear.spark.core.udf.UDF
import com.ysear.spark.spi.datasource.BaseDataSource
import com.ysear.spark.spi.sink.AbsFlowSink
import com.ysear.spark.spi.source.AbsFlowSource
import com.ysear.spark.spi.{DataSourceManager, PluginManager}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SQLContext, UDFRegistration}
import org.dom4j.Document

import java.io.IOException
import java.util
import java.util.concurrent.CopyOnWriteArrayList
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
 * @ClassName YseraFlow.scala
 * @createTime 2022年12月26日 17:31:00
 */
class YseraFlow extends Serializable with Logging{

  protected var conf: Configuration = _

  protected var sqlContext: SQLContext = _

  protected val sparkListener:SparkListener = new FlowSparkListener

  protected var udfRegistration: UDFRegistration = _

  protected val udfs:CopyOnWriteArrayList[UDF] = new CopyOnWriteArrayList[UDF]()

  protected val dataSourceManager:DataSourceManager[BaseDataSource] = new DataSourceManagerImpl

  protected var globalParameter: util.Map[String, String] = _

  protected var sparkConfig:mutable.HashMap[String, String] = _

  var _xml: Document = _

  var _info:FlowInfo = _


  def this(xml:Document,info:FlowInfo,conf: Configuration, sqlContext: SQLContext) {
    this()
    this._xml = xml
    this.conf = conf
    this._info = info
    this.sqlContext = sqlContext
    this.udfRegistration = sqlContext.udf
    this.sqlContext.sparkContext.addSparkListener(this.sparkListener.asInstanceOf[SparkListener])
  }

  val pluginManager:PluginManager = new PluginManager(new YsearSpiLoaderImpl)

  @throws[IOException]
  def execute(xmlFile: Path, userParams: util.Map[String, String]): Int = {
    var execute = 0
    val fs: FileSystem = FileSystem.get(xmlFile.toUri, this.conf)
    val in: FSDataInputStream = fs.open(xmlFile)
    try {
      execute = this.execute(userParams)
      if (in != null) in.close()
    } catch {
      case throwable: Throwable =>
        if (in != null) try in.close()
        catch {
          case throwable1: Throwable =>
            throwable.addSuppressed(throwable1)
        }
        throw throwable
    }
    execute
  }

  @throws[IOException]
  def execute(userParams: util.Map[String, String]): Int = {
    var status:String = "RUNNING"
    var ex:Throwable = null
    try{
      logInfo("123123123")
      val ignoreCase: Boolean = "true".equalsIgnoreCase(this.conf.get("flow.properties.ignore_case"))
      val reader: SparkFlowReader = new SparkFlowReader(_xml, ignoreCase)
      this.globalParameter = ImmutableMap.copyOf(userParams)
      registerUdf(reader)
      val dataSourceReader: FlowDataSourceReader = new FlowDataSourceReader(_xml,pluginManager)
      while (dataSourceReader.hasMoreElements) {
        val dataSource: BaseDataSource = dataSourceReader.nextElement()
        dataSourceManager.registerDataSource(dataSource)
      }
      val sparkSourceNodeReader:FlowSourceReader = new FlowSourceReader(_xml,pluginManager)

      while (sparkSourceNodeReader.hasMoreElements) {
        val source: AbsFlowSource = sparkSourceNodeReader.nextElement()
        source.sqlContext_=(sqlContext)
        source.conf_=(conf)
        source.pluginManager_=(dataSourceManager)
        if(source.isStream){
          throw new RuntimeException("stream source is not support")
        }
        source.createDataFrame
      }
      submitFlow()
      status = "SUCCESS"
    }catch {
      case ex:Exception =>
        status = "FAILED"
        ex.printStackTrace()
    }
    0
  }

  def submitFlow(): Unit = {
    // 处理transform
    val transformReader: FlowTransformReader = new FlowTransformReader(_xml)
    while (transformReader.hasMoreElements) {
      val transformNode: AbsTransform = transformReader.nextElement()
      transformNode.sqlContext_=(sqlContext)
      transformNode.conf_=(conf)
      transformNode.transform
    }
    // 处理target
    val sinkReader: FlowSinkReader = new FlowSinkReader(_xml,pluginManager)
    while (sinkReader.hasMoreElements) {
      val sink: AbsFlowSink = sinkReader.nextElement()
      sink.sqlContext_=(sqlContext)
      sink.conf_=(conf)
      val sql = s"select * from ${sink.viewName}"
      logInfo(s"sink execute sql: ${sql}")
      sink.sink(sqlContext.sql(sql))
      logInfo(s"output target success !!!!")
    }
  }

  val registerUdf: SparkFlowReader => Unit = (reader:SparkFlowReader)=> {
    // 注册udf
    var udfs: ListBuffer[UDF] = null
    udfs = reader.getUdfs
    if (udfs.nonEmpty) {
      udfs.foreach((elem:UDF)=>{
        val udfObject: Any = elem.getUdfObject
        udfObject match {
          case udf: UDF0[_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF1[_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF2[_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF3[_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF4[_, _, _, _, _] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF5[_,_,_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF6[_,_,_,_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF7[_,_,_,_,_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF8[_,_,_,_,_,_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UDF9[_,_,_,_,_,_,_,_,_,_] =>
            this.udfRegistration.register(elem.getName, udf, elem.getSparkDataType)
          case udf: UserDefinedFunction =>
            this.udfRegistration.register(elem.getName, udf)
        }
      })
    }
  }
}
