package com.ysear.spark.core

import com.ysear.spark.common.Constants
import com.ysear.spark.core.flow.{FlowInfo, SparkFlowReader}
import com.ysear.spark.core.principal.{HadoopPrincipal, HadoopPrincipalFactory}
import org.apache.commons.cli.{Options, ParseException}
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, LocalFileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.dom4j.io.SAXReader
import org.dom4j.{Document, Element, Node}

import java.io.{File, IOException}
import java.util
import scala.collection.mutable

/*
 * @Author Administrator
 * @Date 2022/12/25
 **/

class YseraSparkApplication extends Logging {
  def execute(xmlFile: Path, principalTmp: String, keytabFile: String, krb5File: String, params: mutable.HashMap[String, String]) {
    var principal: String = principalTmp;
    log.info("user.principal: {}", principal)
    var keytabPath: String = null
    var krb5Path: String = null
    if (StringUtils.isNotBlank(principal)) {
      var userKeytabFile = new File(keytabFile)
      var userKrb5ConfFile = new File(krb5File)
      if (userKeytabFile.exists) {
        log.info(s"${principal} exists keytab file : ${userKeytabFile.getAbsolutePath}")
      } else {
        userKeytabFile = if (StringUtils.isBlank(keytabFile)) userKeytabFile
        else new File(keytabFile)
      }
      if (userKrb5ConfFile.exists) {
        log.info(s"${principal} exists krb5 conf file: ${userKrb5ConfFile.getAbsolutePath}")
      } else {
        userKrb5ConfFile = if (StringUtils.isBlank(krb5File)) userKrb5ConfFile
        else new File(krb5File)
      }
      if (userKeytabFile.exists && userKrb5ConfFile.exists) {
        keytabPath = userKeytabFile.getAbsolutePath
        krb5Path = userKrb5ConfFile.getAbsolutePath
      }
      else {
        log.warn("userKeytabFile or userKrb5ConfFile not exists...")
        log.warn("set userPrincipal: null")
        principal = null
      }
    }
    val hadoopPrincipal: HadoopPrincipal = HadoopPrincipalFactory.getHadoopPrincipal(principal, keytabPath, krb5Path)
    val secConf: Configuration = hadoopPrincipal.getConf(new util.ArrayList[String]())
    val fs: LocalFileSystem = FileSystem.newInstanceLocal(secConf)
    if (!fs.exists(xmlFile) || !fs.isFile(xmlFile)) throw new ParseException("xmlFilePath can't found file.")
    val in: FSDataInputStream = fs.open(xmlFile)
    try {
      val xml: Document = new SAXReader().read(in)
      val reader = new SparkFlowReader(xml, false)
      val info: FlowInfo = reader.readFlowInfo()
      val sparkConfig: mutable.HashMap[String, String] = reader.getSparkConf
      val builder: SparkSession.Builder = SparkSession.builder
        .master("local[*]")
      //  设置配置
      if (null != sparkConfig && sparkConfig.nonEmpty) {
        sparkConfig.foreach((entity: (String, String)) =>{
          builder.config(entity._1,entity._2)
        })
      }
      // 设置名称
      if(StringUtils.isNotBlank(info.name)){
        builder.appName(info.name)
      }
      // 启用hive支持
      if (enableHive(xml)) {
        log.info("enable hive support!!!")
        builder.enableHiveSupport
      }
      val spark: SparkSession = builder.getOrCreate
      val sqlContext: SQLContext = spark.sqlContext
      val conf: Configuration = spark.sparkContext.hadoopConfiguration
      val iterable: util.Iterator[util.Map.Entry[String, String]] = conf.iterator()
      import scala.collection.JavaConversions._
      while (iterable.hasNext) {
        val entity: util.Map.Entry[String, String] = iterable.next()
        val value: String = secConf.get(entity.getKey)
        if (value == null) secConf.set(entity.getKey, entity.getValue)
      }

      for (entry <- params.entrySet) {
        secConf.set(entry.getKey, entry.getValue)
      }
      val flows = new YseraFlow(xml,info,secConf, sqlContext)
      flows.execute(params)
      if (debug) {
        System.out.println("Press Any Key To Continue.")
        System.in.read
      }
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
  }


  /**
   * 判断是否使用到hive，通过搜索source和sink节点，判断是否存在存在hive类型的数据源
   *
   * @param fs
   * @param xmlFile
   * @throws
   * @return
   */
  @throws[IOException]
  private def enableHive(document:Document): Boolean = {
    try {
      val sources: util.List[Node] = document.selectNodes(Constants.SOURCE_NODE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
      import scala.collection.JavaConversions._
      for (source <- sources) {
        val element: Element = source.asInstanceOf[Element]
        val `type`: String = element.attributeValue("type")
        if ("hive".equalsIgnoreCase(`type`)) {
          log.info("spark session enable hive support.")
          val bool = true
          return bool
        }
      }
      val targets: util.List[Node] = document.selectNodes(Constants.SINK_NODE_XPATH_EXPRESSION).asInstanceOf[util.List[Node]]
      import scala.collection.JavaConversions._
      for (target <- targets) {
        val element: Element = target.asInstanceOf[Element]
        val `type`: String = element.attributeValue("type")
        if ("hive".equalsIgnoreCase(`type`)) {
          log.info("spark session enable hive support.")
          val bool = true
          return bool
        }
      }
    } catch {
      case throwable: Throwable =>
        throw throwable
    }
    false
  }


  private[this] var _debug = true

  private def debug = _debug

  private def debug_=(value: Boolean): Unit = {
    _debug = value
  }


}


object YseraSparkApplication extends Logging {

  def main(args: Array[String]): Unit = {
    log.info("start spark flow!!!")
    val opts = new Options()
    opts.addOption("s", false, "print exception stack.")
    opts.addOption("f", "xml-file", true, "Flow XML File Path")
    opts.addOption("p", "param", true, "Flow XML User Params")
    opts.addOption("i", "ignoreCase", false, "Flow XML Properties Ignore Case")
    opts.addOption(null, "principal", true, "principal for hadoop(Huawei)")
    opts.addOption(null, "keytabFile", true, "keytab File Path(Huawei)")
    opts.addOption(null, "krb5File", true, "krb5 File Path(Huawei)")
    opts.addOption(null, "debug", false, "Debug Mode")

    var throwError = false
    var debugMode = false

    try {
      import org.apache.commons.cli.{CommandLine, GnuParser}
      val cl: CommandLine = (new GnuParser).parse(opts, args)
      throwError = cl.hasOption("s")
      debugMode = cl.hasOption("debug")
      val xmlFilePath: String = cl.getOptionValue("f")
      if (StringUtils.isBlank(xmlFilePath)) throw new ParseException("--xml-file must not be null.")
      val ignoreCase: Boolean = cl.hasOption("ignoreCase")
      val params = new mutable.HashMap[String, String]()
      params.put("flow.properties.ignore_case", String.valueOf(ignoreCase))
      val paramStr: Array[String] = cl.getOptionValues("p")
      if (paramStr != null && paramStr.length > 0) {
        for (tmp <- paramStr) {
          var p: String = tmp;
          if (p.startsWith("'") || p.startsWith("\"")) {
            p = p.substring(1)
          }
          if (p.endsWith("'") || p.endsWith("\"")) {
            p = p.substring(0, p.length - 1)
          }
          val index: Int = p.indexOf("=")
          if (index <= 0) throw new IllegalArgumentException("error user param: " + p + ", please use: -p 'a=b' -p for=bar")
          val k: String = p.substring(0, index)
          val v: String = p.substring(index + 1)
          params.put(k, v)
          log.info(s"add user param: ${k}=${v}")
        }
      }
      val xmlFile = new Path(xmlFilePath)
      log.info("xmlFile path: {}.", xmlFilePath)
      val application: YseraSparkApplication = new YseraSparkApplication()

      application.debug_=(debugMode)
      application.execute(xmlFile,
        cl.getOptionValue("principal"),
        cl.getOptionValue("keytabFile"),
        cl.getOptionValue("krb5File"),
        params)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    def printHelpFor(opts: Options) {
      import org.apache.commons.cli.HelpFormatter
      val f = new HelpFormatter
      f.printHelp("spark-submit --jars etl-flow.jar --class com.yiidata.etl.flow.FlowRunner", "", opts, null)
    }
  }
}
